import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { useChat as useAiSdkChat } from '@ai-sdk/react'
import { useChat as useTanStackChat } from '@tanstack/ai-react'
import type { NatsRuntimeConnectionState } from '@natsail/core'
import { consumeJetStream, type JetStreamDuplicateDeliveryPolicy } from '@natsail/jetstream'
import {
  ArrowUpIcon,
  BotIcon,
  CableIcon,
  CheckIcon,
  CircleAlertIcon,
  MessageCircleIcon,
  RefreshCwIcon,
  SignalIcon,
  TerminalSquareIcon,
  WifiOffIcon,
} from 'lucide-react'

import { Alert, AlertDescription, AlertTitle } from '@natsail/example-chat-ui/ui/alert'
import { Avatar, AvatarBadge, AvatarFallback } from '@natsail/example-chat-ui/ui/avatar'
import { Badge } from '@natsail/example-chat-ui/ui/badge'
import { Bubble, BubbleContent } from '@natsail/example-chat-ui/ui/bubble'
import { Button } from '@natsail/example-chat-ui/ui/button'
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from '@natsail/example-chat-ui/ui/empty'
import { Field, FieldLabel } from '@natsail/example-chat-ui/ui/field'
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupTextarea,
} from '@natsail/example-chat-ui/ui/input-group'
import { Marker, MarkerContent, MarkerIcon } from '@natsail/example-chat-ui/ui/marker'
import {
  Message,
  MessageAvatar,
  MessageContent,
  MessageFooter,
  MessageHeader,
} from '@natsail/example-chat-ui/ui/message'
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from '@natsail/example-chat-ui/ui/message-scroller'
import { Spinner } from '@natsail/example-chat-ui/ui/spinner'
import { ToggleGroup, ToggleGroupItem } from '@natsail/example-chat-ui/ui/toggle-group'
import { useNatsRuntimeStatus } from '@natsail/react'
import { runtime } from './runtime'
import {
  NatsAiSdkChatTransport,
  NatsTanStackConnection,
  type DeliveryKind,
  type FrameworkKind,
  type TransportReceipt,
} from './transports'

const gatewayPrompt = 'Help me plan the gateway release.'
const reconnectPrompt = "What happens if the connection drops while you're answering?"
const conversationStream = 'NATSAIL_AI_CONVERSATIONS'
const conversationSubject = 'natsail.examples.ai.conversations.release-room'
const clientId = `ai-chat-${crypto.randomUUID().slice(0, 8)}`
const encoder = new TextEncoder()

interface Receipt extends TransportReceipt {
  id: number
  at: string
}

interface DisplayMessage {
  id: string
  role: string
  author?: string
  text: string
  reasoning: string
  sequence?: number
}

interface ConversationMessageFrame {
  type: 'message'
  id: string
  phase: 'history' | 'live'
  role: 'user' | 'assistant'
  author: string
  text: string
}

interface ConversationReadyFrame {
  type: 'history-ready'
  id: string
}

type ConversationFrame = ConversationMessageFrame | ConversationReadyFrame

interface ConversationStreamState {
  history: DisplayMessage[]
  live: DisplayMessage[]
  cursor?: number
  loading: boolean
  error?: Error
  injectMessage: () => Promise<void>
}

type ReconnectPhase = 'idle' | 'reconnecting' | 'recovering' | 'reconnected'

const decodeConversationFrame = (data: Uint8Array): ConversationFrame => {
  const value: unknown = JSON.parse(new TextDecoder().decode(data))
  if (!value || typeof value !== 'object') throw new Error('Invalid conversation event')
  const frame = value as Partial<ConversationFrame>
  if (frame.type === 'history-ready' && typeof frame.id === 'string') {
    return frame as ConversationReadyFrame
  }
  if (
    frame.type === 'message' &&
    typeof frame.id === 'string' &&
    (frame.phase === 'history' || frame.phase === 'live') &&
    (frame.role === 'user' || frame.role === 'assistant') &&
    typeof frame.author === 'string' &&
    typeof frame.text === 'string'
  ) {
    return frame as ConversationMessageFrame
  }
  throw new Error('Unknown conversation event')
}

const randomStreamMessages = [
  ['Mira', 'I published this while the local conversation stream was already running.'],
  ['Release bot', 'The retained stream accepted another message without restarting the chat.'],
  ['NATS operator', 'This delivery arrived with a real JetStream stream sequence.'],
] as const

function useConversationStream(): ConversationStreamState {
  const [history, setHistory] = useState<DisplayMessage[]>([])
  const [live, setLive] = useState<DisplayMessage[]>([])
  const [cursor, setCursor] = useState<number>()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<Error>()

  useEffect(() => {
    const lease = consumeJetStream(
      runtime,
      {
        stream: conversationStream,
        filter: conversationSubject,
        start: 'all',
        maxBufferedMessages: 32,
        duplicateDeliveryPolicy: 'drop',
        decode: (message) => decodeConversationFrame(message.data),
      },
      ({ value, cursor: nextCursor }) => {
        setCursor(nextCursor.sequence)
        if (value.type === 'history-ready') {
          setLoading(false)
          return
        }
        const message: DisplayMessage = {
          id: value.id,
          role: value.role,
          author: value.author,
          text: value.text,
          reasoning: '',
          sequence: nextCursor.sequence,
        }
        const update = (current: DisplayMessage[]) =>
          current.some((item) => item.id === message.id) ? current : [...current, message]
        if (value.phase === 'history') setHistory(update)
        else setLive(update)
      }
    )
    lease.ready.catch((cause) => {
      setLoading(false)
      setError(cause instanceof Error ? cause : new Error(String(cause)))
    })
    lease.closed.catch((cause) => {
      setError(cause instanceof Error ? cause : new Error(String(cause)))
    })
    return () => {
      void lease.close()
    }
  }, [])

  const injectMessage = useCallback(async () => {
    try {
      const [author, text] =
        randomStreamMessages[Math.floor(Math.random() * randomStreamMessages.length)]!
      await runtime.publish(
        conversationSubject,
        encoder.encode(
          JSON.stringify({
            type: 'message',
            id: `injected-${crypto.randomUUID()}`,
            phase: 'live',
            role: 'assistant',
            author,
            text,
          } satisfies ConversationMessageFrame)
        )
      )
    } catch (cause) {
      setError(cause instanceof Error ? cause : new Error(String(cause)))
    }
  }, [])

  return {
    history,
    live,
    ...(cursor === undefined ? {} : { cursor }),
    loading,
    ...(error === undefined ? {} : { error }),
    injectMessage,
  }
}

const partContent = (part: unknown, key: 'text' | 'content'): string => {
  if (!part || typeof part !== 'object') return ''
  const value = (part as Record<string, unknown>)[key]
  return typeof value === 'string' ? value : ''
}

const toDisplayMessages = (messages: ReadonlyArray<unknown>): DisplayMessage[] =>
  messages.flatMap((value, index) => {
    if (!value || typeof value !== 'object') return []
    const message = value as Record<string, unknown>
    const parts = Array.isArray(message.parts) ? message.parts : []
    const text = parts
      .filter(
        (part) =>
          part && typeof part === 'object' && (part as Record<string, unknown>).type === 'text'
      )
      .map((part) => (partContent(part, 'text') || partContent(part, 'content')).trim())
      .join('\n\n')
    const reasoning = parts
      .filter(
        (part) =>
          part && typeof part === 'object' && (part as Record<string, unknown>).type === 'reasoning'
      )
      .map((part) => partContent(part, 'text') || partContent(part, 'content'))
      .join('')
    return [
      {
        id: typeof message.id === 'string' ? message.id : `message-${index}`,
        role: typeof message.role === 'string' ? message.role : 'unknown',
        text,
        reasoning,
      },
    ]
  })

const connectionLabel = (state: NatsRuntimeConnectionState): string => {
  switch (state) {
    case 'connected':
      return 'Online'
    case 'disconnected':
    case 'reconnecting':
      return 'Reconnecting'
    case 'closed':
      return 'Offline'
    default:
      return 'Connecting'
  }
}

function AssistantAvatar({ online }: { online: boolean }) {
  return (
    <Avatar>
      <AvatarFallback>N/</AvatarFallback>
      {online ? (
        <AvatarBadge>
          <CheckIcon aria-hidden="true" />
        </AvatarBadge>
      ) : null}
    </Avatar>
  )
}

function ConversationWelcome({ onSend }: { onSend: (message: string) => Promise<void> }) {
  return (
    <Empty className="chat-welcome">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <MessageCircleIcon />
        </EmptyMedia>
        <EmptyTitle className="text-balance">Start a conversation with NATSail</EmptyTitle>
        <EmptyDescription className="text-pretty">
          This is a deterministic local assistant. Its responses still stream through the same NATS
          transport a real model would use.
        </EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        <Button type="button" variant="outline" onClick={() => void onSend(gatewayPrompt)}>
          <BotIcon data-icon="inline-start" />
          {gatewayPrompt}
        </Button>
      </EmptyContent>
    </Empty>
  )
}

function ConnectionMarker({
  phase,
  delivery,
  retainedFrames,
  gapDurationMs,
}: {
  phase: ReconnectPhase
  delivery: DeliveryKind
  retainedFrames: number
  gapDurationMs?: number
}) {
  if (phase === 'idle') return null
  return (
    <MessageScrollerItem messageId={`connection-${phase}`}>
      <Marker
        variant="separator"
        data-reconnect-state={phase}
        data-retained-frames={retainedFrames}
      >
        <MarkerIcon>
          {phase === 'reconnecting' || phase === 'recovering' ? <RefreshCwIcon /> : <CheckIcon />}
        </MarkerIcon>
        <MarkerContent>
          {phase === 'reconnecting'
            ? 'Connection interrupted. JetStream is retaining the reply…'
            : phase === 'recovering'
              ? 'Connection restored. Replaying frames retained during the outage…'
              : delivery === 'jetstream'
                ? `Recovery complete · ${retainedFrames} frames retained during ${(
                    (gapDurationMs ?? 0) / 1_000
                  ).toFixed(2)}s network gap.`
                : 'Connection restored. Live delivery resumed.'}
        </MarkerContent>
      </Marker>
    </MessageScrollerItem>
  )
}

function TranscriptMessage({
  message,
  connected,
  busy = false,
  last = false,
}: {
  message: DisplayMessage
  connected: boolean
  busy?: boolean
  last?: boolean
}) {
  const user = message.role === 'user'
  return (
    <MessageScrollerItem messageId={message.id} scrollAnchor={user}>
      <Message align={user ? 'end' : 'start'}>
        {!user ? (
          <MessageAvatar className="self-start">
            <AssistantAvatar online={connected} />
          </MessageAvatar>
        ) : null}
        <MessageContent>
          <MessageHeader>{message.author ?? (user ? 'You' : 'NATSail Assistant')}</MessageHeader>
          {message.reasoning ? (
            <details className="chat-reasoning">
              <summary>Reasoning</summary>
              <p className="text-pretty">{message.reasoning}</p>
            </details>
          ) : null}
          {message.text ? (
            <Bubble align={user ? 'end' : 'start'} variant={user ? 'default' : 'ghost'}>
              <BubbleContent className="chat-message-copy">{message.text}</BubbleContent>
            </Bubble>
          ) : null}
          <MessageFooter>
            {message.sequence !== undefined
              ? `JetStream · sequence ${message.sequence}`
              : user
                ? 'Sent'
                : busy && last
                  ? 'Streaming through NATS…'
                  : 'Delivered'}
          </MessageFooter>
        </MessageContent>
      </Message>
    </MessageScrollerItem>
  )
}

function NativeTranscript({
  messages,
  busy,
  connected,
  reconnectPhase,
  delivery,
  conversation,
  retainedFrames,
  gapDurationMs,
  onSend,
}: {
  messages: ReadonlyArray<unknown>
  busy: boolean
  connected: boolean
  reconnectPhase: ReconnectPhase
  delivery: DeliveryKind
  conversation: ConversationStreamState
  retainedFrames: number
  gapDurationMs?: number
  onSend: (message: string) => Promise<void>
}) {
  const displayMessages = toDisplayMessages(messages)
  const lastMessage = displayMessages.at(-1)
  const waitingForAssistant = busy && lastMessage?.role === 'user'

  return (
    <MessageScrollerProvider autoScroll>
      <MessageScroller className="chat-transcript">
        <MessageScrollerViewport>
          <MessageScrollerContent className="chat-transcript__content">
            {conversation.loading ? (
              <Marker variant="separator">
                <MarkerIcon>
                  <Spinner />
                </MarkerIcon>
                <MarkerContent>Loading earlier messages from JetStream…</MarkerContent>
              </Marker>
            ) : conversation.history.length > 0 ? (
              <Marker variant="separator" data-history-loaded="true">
                <MarkerContent>
                  Earlier messages · loaded through stream sequence {conversation.cursor}
                </MarkerContent>
              </Marker>
            ) : null}
            {conversation.history.map((message) => (
              <TranscriptMessage key={message.id} message={message} connected={connected} />
            ))}
            {!conversation.loading &&
            conversation.history.length === 0 &&
            displayMessages.length === 0 ? (
              <ConversationWelcome onSend={onSend} />
            ) : null}
            {displayMessages.map((message, index) => (
              <TranscriptMessage
                key={message.id}
                message={message}
                connected={connected}
                busy={busy}
                last={index === displayMessages.length - 1}
              />
            ))}
            {conversation.live.length > 0 ? (
              <Marker variant="separator" data-live-stream-message="true">
                <MarkerContent>
                  New messages published directly to the conversation stream
                </MarkerContent>
              </Marker>
            ) : null}
            {conversation.live.map((message) => (
              <TranscriptMessage key={message.id} message={message} connected={connected} />
            ))}
            {waitingForAssistant ? (
              <MessageScrollerItem messageId="assistant-thinking">
                <Message align="start">
                  <MessageAvatar className="self-start">
                    <AssistantAvatar online={connected} />
                  </MessageAvatar>
                  <MessageContent>
                    <MessageHeader>NATSail Assistant</MessageHeader>
                    <Bubble variant="ghost">
                      <BubbleContent>
                        <span className="shimmer">Thinking through the live stream…</span>
                      </BubbleContent>
                    </Bubble>
                  </MessageContent>
                </Message>
              </MessageScrollerItem>
            ) : null}
            <ConnectionMarker
              phase={reconnectPhase}
              delivery={delivery}
              retainedFrames={retainedFrames}
              {...(gapDurationMs === undefined ? {} : { gapDurationMs })}
            />
          </MessageScrollerContent>
        </MessageScrollerViewport>
        <MessageScrollerButton />
      </MessageScroller>
    </MessageScrollerProvider>
  )
}

function ChatComposer({
  connected,
  busy,
  showStarter,
  showFollowUp,
  onSend,
}: {
  connected: boolean
  busy: boolean
  showStarter: boolean
  showFollowUp: boolean
  onSend: (message: string) => Promise<void>
}) {
  const [message, setMessage] = useState('')
  const [error, setError] = useState<string>()

  const send = async (value: string) => {
    const body = value.trim()
    if (!body || !connected || busy) return
    setError(undefined)
    try {
      await onSend(body)
      setMessage('')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause))
    }
  }

  const submit = (event: FormEvent) => {
    event.preventDefault()
    void send(message)
  }

  return (
    <div className="chat-composer-wrap">
      {showStarter || showFollowUp ? (
        <div className="chat-follow-up">
          <span>{showFollowUp ? 'Suggested follow-up' : 'Continue this conversation'}</span>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => void send(showFollowUp ? reconnectPrompt : gatewayPrompt)}
          >
            {showFollowUp ? reconnectPrompt : gatewayPrompt}
          </Button>
        </div>
      ) : null}
      <form className="chat-composer" onSubmit={submit}>
        <Field
          data-disabled={!connected || busy || undefined}
          data-invalid={error ? true : undefined}
        >
          <FieldLabel htmlFor="chat-message" className="sr-only">
            Message NATSail Assistant
          </FieldLabel>
          <InputGroup>
            <InputGroupTextarea
              id="chat-message"
              name="message"
              rows={2}
              value={message}
              disabled={!connected || busy}
              aria-invalid={error ? true : undefined}
              placeholder={connected ? 'Message NATSail Assistant' : 'Waiting for NATS…'}
              onChange={(event) => setMessage(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' && !event.shiftKey) {
                  event.preventDefault()
                  event.currentTarget.form?.requestSubmit()
                }
              }}
            />
            <InputGroupAddon align="block-end">
              <span>
                {busy ? 'Response streaming' : 'Enter to send · Shift+Enter for a new line'}
              </span>
              <InputGroupButton
                type="submit"
                variant="default"
                size="icon-sm"
                aria-label="Send message"
                disabled={!connected || busy || !message.trim()}
              >
                {busy ? <Spinner /> : <ArrowUpIcon />}
              </InputGroupButton>
            </InputGroupAddon>
          </InputGroup>
          {error ? <p className="chat-composer__error">{error}</p> : null}
        </Field>
      </form>
    </div>
  )
}

function DeveloperDetails({
  framework,
  delivery,
  duplicatePolicy,
  conversation,
  receipts,
  reconnects,
  onFrameworkChange,
  onDeliveryChange,
  onDuplicatePolicyChange,
}: {
  framework: FrameworkKind
  delivery: DeliveryKind
  duplicatePolicy: JetStreamDuplicateDeliveryPolicy
  conversation: ConversationStreamState
  receipts: Receipt[]
  reconnects: number
  onFrameworkChange: (framework: FrameworkKind) => void
  onDeliveryChange: (delivery: DeliveryKind) => void
  onDuplicatePolicyChange: (policy: JetStreamDuplicateDeliveryPolicy) => void
}) {
  const received = receipts.filter((receipt) => receipt.direction === 'receive')
  const completed = receipts.some((receipt) => receipt.direction === 'complete')
  const replySubject = received.at(-1)?.subject
  const cursor = received.at(-1)?.sequence

  return (
    <details className="chat-developer">
      <summary>
        <TerminalSquareIcon aria-hidden="true" />
        Transport details
        <Badge variant={delivery === 'jetstream' ? 'secondary' : 'outline'}>
          {delivery === 'jetstream' ? 'JetStream' : 'Core'}
        </Badge>
        <Badge variant="outline">{framework === 'ai-sdk' ? 'AI SDK' : 'TanStack AI'}</Badge>
      </summary>
      <div className="chat-developer__content">
        <div className="chat-developer__option">
          <span>Delivery mode</span>
          <ToggleGroup
            aria-label="NATS delivery mode"
            value={[delivery]}
            variant="outline"
            size="sm"
            onValueChange={(values) => {
              const value = values.at(-1)
              if (value === 'core' || value === 'jetstream') onDeliveryChange(value)
            }}
          >
            <ToggleGroupItem value="jetstream">JetStream</ToggleGroupItem>
            <ToggleGroupItem value="core">Core</ToggleGroupItem>
          </ToggleGroup>
        </div>
        <div className="chat-developer__option">
          <span>Framework adapter</span>
          <ToggleGroup
            aria-label="Chat framework adapter"
            value={[framework]}
            variant="outline"
            size="sm"
            onValueChange={(values) => {
              const value = values.at(-1)
              if (value === 'ai-sdk' || value === 'tanstack-ai') onFrameworkChange(value)
            }}
          >
            <ToggleGroupItem value="ai-sdk">AI SDK</ToggleGroupItem>
            <ToggleGroupItem value="tanstack-ai">TanStack AI</ToggleGroupItem>
          </ToggleGroup>
        </div>
        <div className="chat-developer__option">
          <span>Duplicate delivery policy</span>
          <ToggleGroup
            aria-label="JetStream duplicate policy"
            value={[duplicatePolicy]}
            variant="outline"
            size="sm"
            disabled={delivery !== 'jetstream'}
            onValueChange={(values) => {
              const value = values.at(-1)
              if (value === 'drop' || value === 'deliver' || value === 'error') {
                onDuplicatePolicyChange(value)
              }
            }}
          >
            <ToggleGroupItem value="drop">Drop</ToggleGroupItem>
            <ToggleGroupItem value="deliver">Deliver</ToggleGroupItem>
            <ToggleGroupItem value="error">Error</ToggleGroupItem>
          </ToggleGroup>
        </div>
        <div className="chat-developer__option">
          <span>Local conversation stream</span>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={!conversation.cursor}
            onClick={() => void conversation.injectMessage().catch(() => undefined)}
          >
            Inject random message
          </Button>
        </div>
        <dl>
          <div>
            <dt>Native chunks</dt>
            <dd className="tabular-nums">{received.length}</dd>
          </div>
          <div>
            <dt>Reconnects</dt>
            <dd className="tabular-nums">{reconnects}</dd>
          </div>
          <div>
            <dt>Reply cursor</dt>
            <dd className="tabular-nums">{cursor ?? 'Live only'}</dd>
          </div>
          <div>
            <dt>History cursor</dt>
            <dd className="tabular-nums">{conversation.cursor ?? 'Loading'}</dd>
          </div>
          <div>
            <dt>Ack policy</dt>
            <dd>{delivery === 'jetstream' ? 'None' : 'N/A'}</dd>
          </div>
          <div>
            <dt>Duplicates</dt>
            <dd className="tabular-nums">
              {received.filter((receipt) => receipt.duplicate).length}
            </dd>
          </div>
          <div>
            <dt>Start policy</dt>
            <dd>{delivery === 'jetstream' ? 'new / all' : 'live'}</dd>
          </div>
          <div>
            <dt>Stream</dt>
            <dd>{completed ? 'Complete' : received.length > 0 ? 'Active' : 'Idle'}</dd>
          </div>
        </dl>
        {replySubject ? <code>{replySubject}</code> : null}
        {delivery === 'jetstream' ? (
          <p className="text-pretty">
            Ordered consumers require AckPolicy.None. NATSail checkpoints only after the handler
            succeeds, exposes the global stream sequence, and applies the selected duplicate policy.
            Explicit acknowledgements need a separate named durable-consumer API.
          </p>
        ) : (
          <p className="text-pretty">
            Core NATS resumes live delivery after reconnect, but it cannot replay native chunks
            published during the disconnected gap.
          </p>
        )}
      </div>
    </details>
  )
}

interface ChatExperienceProps {
  framework: FrameworkKind
  delivery: DeliveryKind
  duplicatePolicy: JetStreamDuplicateDeliveryPolicy
  conversation: ConversationStreamState
  messages: ReadonlyArray<unknown>
  busy: boolean
  error?: Error
  receipts: Receipt[]
  onSend: (message: string) => Promise<void>
  onInterruptStream: (durationMs: number) => Promise<void>
  onFrameworkChange: (framework: FrameworkKind) => void
  onDeliveryChange: (delivery: DeliveryKind) => void
  onDuplicatePolicyChange: (policy: JetStreamDuplicateDeliveryPolicy) => void
}

function ChatExperience(props: ChatExperienceProps) {
  const runtimeStatus = useNatsRuntimeStatus()
  const connected = runtimeStatus.state === 'connected'
  const [reconnectPhase, setReconnectPhase] = useState<ReconnectPhase>('idle')
  const [reconnects, setReconnects] = useState(0)
  const [reconnectError, setReconnectError] = useState<string>()
  const [reconnectWindow, setReconnectWindow] = useState<{
    startedAt: number
    endedAt?: number
  }>()
  const [usedStarter, setUsedStarter] = useState(false)
  const displayMessages = toDisplayMessages(props.messages)
  const lastPublishIndex = props.receipts
    .map((receipt) => receipt.direction === 'publish')
    .lastIndexOf(true)
  const activeTextChunks = props.receipts
    .slice(lastPublishIndex + 1)
    .filter(
      (receipt) =>
        receipt.direction === 'receive' &&
        (receipt.event === 'text-delta' || receipt.event === 'TEXT_MESSAGE_CONTENT')
    ).length
  const reconnectReady =
    connected &&
    props.delivery === 'jetstream' &&
    props.busy &&
    activeTextChunks >= 6 &&
    reconnectPhase === 'idle'
  const retainedFrames = reconnectWindow?.endedAt
    ? props.receipts.filter(
        (receipt) =>
          receipt.direction === 'receive' &&
          receipt.publishedAt !== undefined &&
          receipt.publishedAt >= reconnectWindow.startedAt &&
          receipt.publishedAt <= reconnectWindow.endedAt!
      ).length
    : 0
  const gapDurationMs = reconnectWindow?.endedAt
    ? reconnectWindow.endedAt - reconnectWindow.startedAt
    : undefined

  const send = async (message: string) => {
    if (reconnectPhase !== 'idle') {
      setReconnectPhase('idle')
      setReconnectWindow(undefined)
    }
    setUsedStarter(message === gatewayPrompt)
    await props.onSend(message)
  }

  const forceReconnect = async () => {
    if (!connected || reconnectPhase !== 'idle') return
    const startedAt = Date.now()
    setReconnectError(undefined)
    setReconnectWindow({ startedAt })
    setReconnectPhase('reconnecting')
    try {
      const streamInterruption = props.onInterruptStream(2_000)
      await new Promise((resolve) => window.setTimeout(resolve, 30))
      const connection = await runtime.connection()
      const observed = (async () => {
        for await (const status of connection.status()) {
          if (status.type === 'disconnect' || status.type === 'reconnecting') {
            setReconnectPhase('reconnecting')
          }
          if (status.type === 'reconnect') return
        }
      })()
      await connection.reconnect()
      await observed
      setReconnects((count) => count + 1)
      setReconnectPhase('recovering')
      await streamInterruption
      setReconnectWindow({ startedAt, endedAt: Date.now() })
      await new Promise((resolve) => window.setTimeout(resolve, 600))
      setReconnectPhase('reconnected')
    } catch (cause) {
      setReconnectPhase('idle')
      setReconnectError(cause instanceof Error ? cause.message : String(cause))
    }
  }

  return (
    <main className="chat-app">
      <header className="chat-app-bar">
        <div className="chat-brand">
          <span className="chat-brand__mark" aria-hidden="true">
            N/
          </span>
          <div>
            <strong>NATSail Chat</strong>
            <span>Private example</span>
          </div>
        </div>
        <Badge
          variant={connected ? 'secondary' : 'outline'}
          data-runtime-status={runtimeStatus.state}
        >
          <SignalIcon data-icon="inline-start" />
          {connectionLabel(runtimeStatus.state)}
        </Badge>
      </header>

      <section className="chat-window" aria-labelledby="chat-title">
        <header className="chat-window__header">
          <div className="chat-person">
            <AssistantAvatar online={connected} />
            <div>
              <h1 id="chat-title">NATSail Assistant</h1>
              <p className="text-pretty">
                {props.delivery === 'jetstream'
                  ? 'Local, deterministic, and resumable through JetStream'
                  : 'Local, deterministic, and streaming through Core NATS'}
              </p>
            </div>
          </div>
          <div className="chat-window__actions">
            <span>
              {props.delivery === 'jetstream'
                ? 'Pauses the ordered consumer for two seconds while publishing continues'
                : 'Switch to JetStream to test gap recovery'}
            </span>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={!reconnectReady}
              onClick={() => void forceReconnect()}
            >
              {reconnectPhase === 'reconnecting' || reconnectPhase === 'recovering' ? (
                <Spinner data-icon="inline-start" />
              ) : (
                <WifiOffIcon data-icon="inline-start" />
              )}
              {reconnectPhase === 'reconnecting'
                ? 'Offline…'
                : reconnectPhase === 'recovering'
                  ? 'Replaying…'
                  : reconnectPhase === 'reconnected'
                    ? 'Recovery complete'
                    : 'Run recovery test'}
            </Button>
          </div>
        </header>

        <NativeTranscript
          messages={props.messages}
          busy={props.busy}
          connected={connected}
          reconnectPhase={reconnectPhase}
          delivery={props.delivery}
          conversation={props.conversation}
          retainedFrames={retainedFrames}
          {...(gapDurationMs === undefined ? {} : { gapDurationMs })}
          onSend={send}
        />

        <footer className="chat-window__footer">
          {!connected && runtimeStatus.state !== 'idle' && runtimeStatus.state !== 'connecting' ? (
            <Alert>
              <CableIcon />
              <AlertTitle>Reconnecting to the conversation</AlertTitle>
              <AlertDescription>
                {props.delivery === 'jetstream'
                  ? 'Your transcript stays in place while JetStream retains the reply.'
                  : 'Your transcript stays in place while NATS reconnects.'}
              </AlertDescription>
            </Alert>
          ) : null}
          {props.error || props.conversation.error || reconnectError ? (
            <Alert variant="destructive">
              <CircleAlertIcon />
              <AlertTitle>The conversation was interrupted</AlertTitle>
              <AlertDescription>
                {props.error?.message ?? props.conversation.error?.message ?? reconnectError}
              </AlertDescription>
            </Alert>
          ) : null}
          <ChatComposer
            connected={connected}
            busy={props.busy}
            showStarter={!props.conversation.loading && displayMessages.length === 0 && !props.busy}
            showFollowUp={usedStarter && displayMessages.length >= 2 && !props.busy}
            onSend={async (message) => {
              if (message === reconnectPrompt) setUsedStarter(false)
              await send(message)
            }}
          />
          <DeveloperDetails
            framework={props.framework}
            delivery={props.delivery}
            duplicatePolicy={props.duplicatePolicy}
            conversation={props.conversation}
            receipts={props.receipts}
            reconnects={reconnects}
            onFrameworkChange={props.onFrameworkChange}
            onDeliveryChange={props.onDeliveryChange}
            onDuplicatePolicyChange={props.onDuplicatePolicyChange}
          />
        </footer>
      </section>
    </main>
  )
}

function AiSdkChat({
  delivery,
  duplicatePolicy,
  conversation,
  receipts,
  onReceipt,
  onFrameworkChange,
  onDeliveryChange,
  onDuplicatePolicyChange,
}: {
  delivery: DeliveryKind
  duplicatePolicy: JetStreamDuplicateDeliveryPolicy
  conversation: ConversationStreamState
  receipts: Receipt[]
  onReceipt: (value: TransportReceipt) => void
  onFrameworkChange: (framework: FrameworkKind) => void
  onDeliveryChange: (delivery: DeliveryKind) => void
  onDuplicatePolicyChange: (policy: JetStreamDuplicateDeliveryPolicy) => void
}) {
  const transport = useMemo(
    () =>
      new NatsAiSdkChatTransport(
        runtime,
        clientId,
        delivery,
        { duplicateDeliveryPolicy: duplicatePolicy },
        onReceipt
      ),
    [delivery, duplicatePolicy, onReceipt]
  )
  const { messages, sendMessage, status, error } = useAiSdkChat({
    id: 'natsail-ai-sdk-chat',
    transport,
  })
  const busy = status === 'submitted' || status === 'streaming'
  return (
    <ChatExperience
      framework="ai-sdk"
      delivery={delivery}
      duplicatePolicy={duplicatePolicy}
      conversation={conversation}
      messages={messages}
      busy={busy}
      {...(error ? { error } : {})}
      receipts={receipts}
      onSend={(message) => sendMessage({ text: message })}
      onInterruptStream={transport.interruptActiveStream}
      onFrameworkChange={onFrameworkChange}
      onDeliveryChange={onDeliveryChange}
      onDuplicatePolicyChange={onDuplicatePolicyChange}
    />
  )
}

function TanStackChat({
  delivery,
  duplicatePolicy,
  conversation,
  receipts,
  onReceipt,
  onFrameworkChange,
  onDeliveryChange,
  onDuplicatePolicyChange,
}: {
  delivery: DeliveryKind
  duplicatePolicy: JetStreamDuplicateDeliveryPolicy
  conversation: ConversationStreamState
  receipts: Receipt[]
  onReceipt: (value: TransportReceipt) => void
  onFrameworkChange: (framework: FrameworkKind) => void
  onDeliveryChange: (delivery: DeliveryKind) => void
  onDuplicatePolicyChange: (policy: JetStreamDuplicateDeliveryPolicy) => void
}) {
  const connection = useMemo(
    () =>
      new NatsTanStackConnection(
        runtime,
        clientId,
        delivery,
        { duplicateDeliveryPolicy: duplicatePolicy },
        onReceipt
      ),
    [delivery, duplicatePolicy, onReceipt]
  )
  useEffect(() => () => void connection.close(), [connection])
  const { messages, sendMessage, isLoading, error } = useTanStackChat({
    id: 'natsail-tanstack-ai-chat',
    connection,
  })
  return (
    <ChatExperience
      framework="tanstack-ai"
      delivery={delivery}
      duplicatePolicy={duplicatePolicy}
      conversation={conversation}
      messages={messages}
      busy={isLoading}
      {...(error ? { error } : {})}
      receipts={receipts}
      onSend={(message) => sendMessage(message)}
      onInterruptStream={connection.interruptActiveStream}
      onFrameworkChange={onFrameworkChange}
      onDeliveryChange={onDeliveryChange}
      onDuplicatePolicyChange={onDuplicatePolicyChange}
    />
  )
}

export function App() {
  const [framework, setFramework] = useState<FrameworkKind>('ai-sdk')
  const [delivery, setDelivery] = useState<DeliveryKind>('jetstream')
  const [duplicatePolicy, setDuplicatePolicy] = useState<JetStreamDuplicateDeliveryPolicy>('drop')
  const [receipts, setReceipts] = useState<Receipt[]>([])
  const conversation = useConversationStream()

  useEffect(() => {
    void runtime.connection().catch(() => undefined)
  }, [])

  const onReceipt = useCallback((receipt: TransportReceipt) => {
    setReceipts((current) =>
      [
        ...current,
        {
          ...receipt,
          id: (current.at(-1)?.id ?? 0) + 1,
          at: new Date().toLocaleTimeString(),
        },
      ].slice(-120)
    )
  }, [])

  const visibleReceipts = receipts.filter(
    (receipt) => receipt.framework === framework && receipt.delivery === delivery
  )
  return framework === 'ai-sdk' ? (
    <AiSdkChat
      key={`ai-sdk:${delivery}:${duplicatePolicy}`}
      delivery={delivery}
      duplicatePolicy={duplicatePolicy}
      conversation={conversation}
      receipts={visibleReceipts}
      onReceipt={onReceipt}
      onFrameworkChange={setFramework}
      onDeliveryChange={setDelivery}
      onDuplicatePolicyChange={setDuplicatePolicy}
    />
  ) : (
    <TanStackChat
      key={`tanstack-ai:${delivery}:${duplicatePolicy}`}
      delivery={delivery}
      duplicatePolicy={duplicatePolicy}
      conversation={conversation}
      receipts={visibleReceipts}
      onReceipt={onReceipt}
      onFrameworkChange={setFramework}
      onDeliveryChange={setDelivery}
      onDuplicatePolicyChange={setDuplicatePolicy}
    />
  )
}
