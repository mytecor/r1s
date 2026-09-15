package rns

import (
	"bytes"
	"context"

	"github.com/Quad4-Software/Reticulum-Go/pkg/channel"
	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func (e *Endpoint) acceptLink(value any) {
	inbound, ok := value.(*link.Link)
	if !ok || inbound == nil {
		return
	}
	active, err := e.newSession(inbound, nil, nil)
	if err != nil {
		inbound.Teardown()
		return
	}
	authenticate := func(remote *identity.Identity) {
		if remote == nil {
			return
		}
		active.mu.Lock()
		active.sender = bytes.Clone(remote.Hash())
		pending := active.pending
		active.pending = nil
		pendingAuth := active.pendingAuth
		active.pendingAuth = nil
		active.mu.Unlock()
		e.connections.cacheSession(active, remote.Hash(), nil)
		e.beginAuthentication(active)
		for _, data := range pendingAuth {
			var message authMessage
			if message.Unpack(data) == nil {
				go e.handleAuthentication(active, &message)
			}
		}
		for _, data := range pending {
			go e.deliver(active, data)
		}
	}
	inbound.SetRemoteIdentifiedCallback(func(_ *link.Link, remote *identity.Identity) { authenticate(remote) })
	authenticate(inbound.GetRemoteIdentity())
}

func (e *Endpoint) newSession(rnsLink *link.Link, sender, destinationHash []byte) (*session, error) {
	rnsChannel := rnsLink.GetChannel()
	if err := rnsChannel.RegisterMessageType(authMessageType, func() channel.MessageBase { return &authMessage{} }); err != nil {
		return nil, err
	}
	if err := rnsChannel.RegisterMessageType(envelopeMessageType, func() channel.MessageBase { return &envelopeMessage{} }); err != nil {
		return nil, err
	}
	active := &session{
		link: rnsLink, channel: rnsChannel,
		sender: bytes.Clone(sender), authDone: make(chan struct{}),
	}
	rnsChannel.AddMessageHandler(func(message channel.MessageBase) bool {
		if authentication, ok := message.(*authMessage); ok {
			go e.handleAuthentication(active, authentication)
			return true
		}
		wire, ok := message.(*envelopeMessage)
		if !ok {
			return false
		}
		go e.deliver(active, wire.data)
		return true
	})
	e.connections.cacheSession(active, sender, destinationHash)
	e.connections.rememberDestination(sender, destinationHash)
	return active, nil
}

func (e *Endpoint) deliver(active *session, data []byte) {
	active.mu.Lock()
	sender := bytes.Clone(active.sender)
	if len(sender) == 0 {
		if len(active.pending) < 8 {
			active.pending = append(active.pending, bytes.Clone(data))
		}
		active.mu.Unlock()
		return
	}
	if !active.authenticated {
		if active.authErr == nil && len(active.pending) < 8 {
			active.pending = append(active.pending, bytes.Clone(data))
		}
		active.mu.Unlock()
		return
	}
	active.mu.Unlock()
	var envelope r1sv1.Envelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		return
	}
	// Authenticated link identity is authoritative; payload identity is ignored.
	envelope.Sender = sender
	if err := protocol.ValidateEnvelope(&envelope); err != nil {
		return
	}
	_ = e.handler(context.Background(), &envelope)
}
