package rns

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"

	"quad4/reticulum-go/pkg/channel"
)

const authDomain = "r1s-auth-v1"

var ErrClusterAuthentication = errors.New("cluster authentication failed")

func authProof(key, nonce, challenger, responder []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(authDomain))
	_, _ = mac.Write(nonce)
	_, _ = mac.Write(challenger)
	_, _ = mac.Write(responder)
	return mac.Sum(nil)
}

func (e *Endpoint) beginAuthentication(active *session) {
	active.mu.Lock()
	if len(active.challenge) != 0 || active.authErr != nil || active.authenticated {
		active.mu.Unlock()
		return
	}
	nonce := make([]byte, authNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		active.mu.Unlock()
		e.completeAuthentication(active, fmt.Errorf("%w: generate nonce: %v", ErrClusterAuthentication, err))
		return
	}
	active.challenge = nonce
	active.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), e.networkWait)
	defer cancel()
	if err := e.sendChannel(ctx, active, &authMessage{kind: authKindChallenge, nonce: nonce}); err != nil {
		e.completeAuthentication(active, fmt.Errorf("%w: send challenge: %v", ErrClusterAuthentication, err))
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
		proof := authProof(e.clusterKey, message.nonce, sender, e.identity.Hash())
		ctx, cancel := context.WithTimeout(context.Background(), e.networkWait)
		defer cancel()
		if err := e.sendChannel(ctx, active, &authMessage{kind: authKindResponse, nonce: message.nonce, proof: proof}); err != nil {
			e.completeAuthentication(active, fmt.Errorf("%w: send response: %v", ErrClusterAuthentication, err))
			active.link.Teardown()
		}
	case authKindResponse:
		if len(challenge) != authNonceSize || !bytes.Equal(challenge, message.nonce) ||
			!hmac.Equal(authProof(e.clusterKey, challenge, e.identity.Hash(), sender), message.proof) {
			e.completeAuthentication(active, ErrClusterAuthentication)
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
			for _, data := range pending {
				go e.deliver(active, data)
			}
		}
	})
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
			return ErrClusterAuthentication
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
