package rns

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/Quad4-Software/Reticulum-Go/pkg/channel"
	"github.com/mytecor/r1s/meshbus"
)

var ErrRealmAuthentication = errors.New("realm authentication failed")

func (e *Endpoint) beginAuthentication(active *session) {
	active.mu.Lock()
	if len(active.challenge) != 0 || active.authErr != nil || active.authenticated {
		active.mu.Unlock()
		return
	}
	nonce := make([]byte, authNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		active.mu.Unlock()
		e.completeAuthentication(active, fmt.Errorf("%w: generate nonce: %v", ErrRealmAuthentication, err))
		return
	}
	active.challenge = nonce
	active.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), e.networkWait)
	defer cancel()
	if err := e.sendChannel(ctx, active, &authMessage{kind: authKindChallenge, nonce: nonce}); err != nil {
		e.completeAuthentication(active, fmt.Errorf("%w: send challenge: %v", ErrRealmAuthentication, err))
		active.link.Teardown()
	}
}

func (e *Endpoint) handleAuthentication(active *session, message *authMessage) {
	active.mu.RLock()
	sender := bytes.Clone(active.sender)
	challenge := bytes.Clone(active.challenge)
	active.mu.RUnlock()
	if len(sender) == 0 {
		data, err := message.Pack()
		if err != nil {
			return
		}
		active.mu.Lock()
		if len(active.pendingAuth) < 8 {
			active.pendingAuth = append(active.pendingAuth, data)
		}
		active.mu.Unlock()
		return
	}
	switch message.kind {
	case authKindChallenge:
		proof, err := e.realm.Proof(message.nonce, sender, e.identity.Hash())
		if err != nil {
			e.completeAuthentication(active, fmt.Errorf("%w: create proof: %v", ErrRealmAuthentication, err))
			active.link.Teardown()
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), e.networkWait)
		defer cancel()
		if err := e.sendChannel(ctx, active, &authMessage{kind: authKindResponse, nonce: message.nonce, proof: proof}); err != nil {
			e.completeAuthentication(active, fmt.Errorf("%w: send response: %v", ErrRealmAuthentication, err))
			active.link.Teardown()
		}
	case authKindResponse:
		if len(challenge) != authNonceSize || !bytes.Equal(challenge, message.nonce) ||
			!e.realm.Verify(message.proof, challenge, e.identity.Hash(), sender) {
			e.completeAuthentication(active, ErrRealmAuthentication)
			active.link.Teardown()
			return
		}
		e.completeAuthentication(active, nil)
	}
}

func (e *Endpoint) sendChannel(ctx context.Context, active *session, message channel.MessageBase) error {
	active.sendMu.Lock()
	defer active.sendMu.Unlock()
	if err := active.channel.WaitReady(ctx); err != nil {
		return err
	}
	return active.channel.Send(message)
}

func (e *Endpoint) completeAuthentication(active *session, err error) {
	active.authOnce.Do(func() {
		active.mu.Lock()
		active.authErr = err
		active.authenticated = err == nil
		pending := active.pending
		active.pending = nil
		active.mu.Unlock()
		close(active.authDone)
		if err == nil {
			e.notifyAuthenticated(active)
			for _, data := range pending {
				go e.deliver(active, data)
			}
		}
	})
}

func (e *Endpoint) notifyAuthenticated(active *session) {
	active.mu.RLock()
	sender := bytes.Clone(active.sender)
	active.mu.RUnlock()
	peer, err := meshbus.NewPeerID(sender)
	if err != nil {
		return
	}
	route, ok := e.DestinationForIdentity(peer.String())
	if !ok {
		route = peer.String()
	}
	e.mu.Lock()
	observer := e.observer
	e.mu.Unlock()
	if observer != nil {
		_ = observer.Authenticated(peer, route)
	}
}

func (e *Endpoint) waitAuthenticated(ctx context.Context, active *session) error {
	select {
	case <-active.authDone:
		active.mu.RLock()
		err := active.authErr
		authenticated := active.authenticated
		active.mu.RUnlock()
		if err != nil {
			return err
		}
		if !authenticated {
			return ErrRealmAuthentication
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
