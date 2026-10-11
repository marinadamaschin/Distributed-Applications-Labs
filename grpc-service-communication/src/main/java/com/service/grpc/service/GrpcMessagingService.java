package com.service.grpc.service;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import com.service.grpc.store.FileMessageStore;
import com.service.grpc.service.*;

import java.io.IOException;

public final class GrpcMessagingService extends MessagingServiceGrpc.MessagingServiceImplBase {

    private final FileMessageStore store;

    public GrpcMessagingService(FileMessageStore store) {
        this.store = store;
    }

    @Override
    public void publish(PublishRequest r, StreamObserver<PublishReply> out) {
        if (r.getMessageId().isBlank() || r.getTopic().isBlank() || r.getSchemaVersion().isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("message_id, topic and schema_version are required").asRuntimeException());
            return;
        }

        try {
            store.publish(r);
            out.onNext(PublishReply.newBuilder().setAccepted(true).setDetail("Durably appended to file journal").build());
            out.onCompleted();
        } catch (FileMessageStore.DuplicateMessageException e) {
            out.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asRuntimeException());
        } catch (IOException e) {
            out.onError(Status.INTERNAL.withDescription("Could not persist message").withCause(e).asRuntimeException());
        }
    }

    @Override
    public void subscribe(SubscribeRequest r, StreamObserver<Delivery> out) {
        if (r.getConsumerId().isBlank() || r.getTopic().isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("consumer_id and topic are required").asRuntimeException());
            return;
        }

        ServerCallStreamObserver<Delivery> observer = (ServerCallStreamObserver<Delivery>) out;

        try {
            store.subscribe(r.getConsumerId(), r.getTopic());
        } catch (IOException e) {
            out.onError(Status.INTERNAL.withCause(e).asRuntimeException());
            return;
        }

        Thread worker = new Thread(() -> {
            try {
                while (!observer.isCancelled()) {
                    for (FileMessageStore.DeliveryCandidate candidate : store.pending(r.getConsumerId(), r.getTopic())) {
                        if (observer.isCancelled()) {
                            return;
                        };

                        FileMessageStore.StoredMessage m = candidate.message();
                        observer.onNext(Delivery.newBuilder().setMessageId(m.messageId()).setTopic(m.topic()).setPayload(m.payload())
                                .setMessageType(m.messageType()).setCorrelationId(m.correlationId()).setOccurredAt(m.occurredAt())
                                .setSchemaVersion(m.schemaVersion()).setAttempt(candidate.attempt()).build());
                    }

                    Thread.sleep(250);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (!observer.isCancelled())
                    observer.onError(Status.INTERNAL.withDescription("Subscription stream failed").withCause(e).asRuntimeException());
            }
        }, "grpc-subscription-" + r.getConsumerId());

        worker.setDaemon(true);
        worker.start();

        observer.setOnCancelHandler(() -> {
            worker.interrupt();
            try {
                store.unsubscribe(r.getConsumerId(), r.getTopic());
            } catch (IOException ignored) {
            }
        });
    }

    @Override
    public void ack(AckRequest r, StreamObserver<AckReply> out) {
        if (r.getConsumerId().isBlank() || r.getMessageId().isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("consumer_id and message_id are required").asRuntimeException());
            return;
        }

        try {
            out.onNext(AckReply.newBuilder().setAccepted(store.ack(r.getConsumerId(), r.getMessageId())).build());
            out.onCompleted();
        } catch (IOException e) {
            out.onError(Status.INTERNAL.withDescription("Could not persist ACK").withCause(e).asRuntimeException());
        }
    }

    @Override
    public void nack(NackRequest r, StreamObserver<AckReply> out) {
        try {
            out.onNext(AckReply.newBuilder().setAccepted(store.nack(r.getConsumerId(), r.getMessageId(), r.getReason())).build());
            out.onCompleted();
        } catch (IOException e) {
            out.onError(Status.INTERNAL.withDescription("Could not persist NACK").withCause(e).asRuntimeException());
        }
    }

    @Override
    public void unsubscribe(UnsubscribeRequest r, StreamObserver<AckReply> out) {
        try {
            store.unsubscribe(r.getConsumerId(), r.getTopic());
            out.onNext(AckReply.newBuilder().setAccepted(true).build());
            out.onCompleted();
        } catch (IOException e) {
            out.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    @Override
    public void getStatus(StatusRequest r, StreamObserver<StatusReply> out) {
        try {
            out.onNext(StatusReply.newBuilder().setMessages(store.messageCount()).setPending(store.pendingCount()).setDeadLetter(store.deadLetterCount()).setSubscriptions(store.subscriptionCount()).build());
            out.onCompleted();
        } catch (IOException e) {
            out.onError(Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }
}