import {
  Profiler,
  startTransition,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from 'react'
import {
  ActivityIcon,
  BellIcon,
  CheckIcon,
  ChevronsUpIcon,
  ExternalLinkIcon,
  GaugeIcon,
  LoaderCircleIcon,
  MessageSquareTextIcon,
  PanelRightCloseIcon,
  PanelRightOpenIcon,
  SearchIcon,
  SendIcon,
  SparklesIcon,
  WifiIcon,
  XIcon,
} from 'lucide-react'

import { Avatar, AvatarFallback } from '#components/ui/avatar'
import { Badge } from '#components/ui/badge'
import { Button } from '#components/ui/button'
import { Input } from '#components/ui/input'
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from '#components/ui/message-scroller'
import type {
  DemoAdapter,
  DemoChatEntry,
  DemoConversation,
  DemoConversationActivity,
  DemoConversationPhase,
  DemoPerformanceMetrics,
  DemoUpdateNotice,
} from './performance-model'

export interface PerformanceChatProps {
  readonly adapter: DemoAdapter
  readonly conversations: readonly DemoConversation[]
  readonly activeConversationId: string
  readonly activity: Readonly<Record<string, DemoConversationActivity>>
  readonly entries: readonly DemoChatEntry[]
  readonly phase: DemoConversationPhase
  readonly metrics: DemoPerformanceMetrics
  readonly revision: number
  readonly clientId: string
  readonly notice?: DemoUpdateNotice
  readonly onConversationChange: (conversationId: string) => void
  readonly onSend: (body: string) => Promise<void>
  readonly onBusyBurst: () => Promise<void>
  readonly onDismissNotice: () => void
  readonly onReactCommit: (revision: number, duration: number) => void
}

const relativeTime = (value: string): string => {
  const elapsed = Date.now() - new Date(value).getTime()
  const minutes = Math.max(0, Math.round(elapsed / 60_000))
  if (minutes < 1) return 'now'
  if (minutes < 60) return `${minutes}m`
  const hours = Math.round(minutes / 60)
  if (hours < 24) return `${hours}h`
  return `${Math.round(hours / 24)}d`
}

const messageTime = (value: string): string =>
  new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit' }).format(new Date(value))

function useTabPresence(adapter: DemoAdapter, clientId: string): number {
  const [count, setCount] = useState(1)

  useEffect(() => {
    const channel = new BroadcastChannel(`natsail-${adapter}-chat-tabs`)
    const peers = new Map<string, number>()
    const refresh = () => {
      const threshold = Date.now() - 4_500
      for (const [id, seenAt] of peers) if (seenAt < threshold) peers.delete(id)
      setCount(1 + peers.size)
    }
    const announce = () => channel.postMessage({ type: 'presence', clientId, at: Date.now() })

    channel.onmessage = (event: MessageEvent<unknown>) => {
      if (!event.data || typeof event.data !== 'object') return
      const message = event.data as { type?: unknown; clientId?: unknown; at?: unknown }
      if (
        message.type !== 'presence' ||
        typeof message.clientId !== 'string' ||
        typeof message.at !== 'number' ||
        message.clientId === clientId
      ) {
        return
      }
      peers.set(message.clientId, message.at)
      refresh()
    }

    announce()
    const heartbeat = window.setInterval(() => {
      announce()
      refresh()
    }, 2_000)
    return () => {
      window.clearInterval(heartbeat)
      channel.close()
    }
  }, [adapter, clientId])

  return count
}

function ConversationList({
  conversations,
  activeConversationId,
  activity,
  onConversationChange,
}: Pick<
  PerformanceChatProps,
  'activeConversationId' | 'activity' | 'conversations' | 'onConversationChange'
>) {
  const [query, setQuery] = useState('')
  const visible = useMemo(() => {
    const normalized = query.trim().toLowerCase()
    if (!normalized) return conversations
    return conversations.filter((conversation) =>
      `${conversation.title} ${conversation.summary}`.toLowerCase().includes(normalized)
    )
  }, [conversations, query])

  return (
    <aside className="chat-inbox" aria-label="Conversations">
      <div className="chat-inbox__brand">
        <span className="chat-inbox__mark">N/</span>
        <div>
          <strong>NATSail Chat</strong>
          <span>Conversation lab</span>
        </div>
      </div>
      <label className="chat-search">
        <SearchIcon aria-hidden="true" />
        <Input
          value={query}
          placeholder="Search conversations"
          onChange={(event) => setQuery(event.target.value)}
        />
      </label>
      <div className="chat-inbox__label">
        <span>Recent</span>
        <Badge variant="outline">{conversations.length}</Badge>
      </div>
      <nav className="conversation-list">
        {visible.map((conversation) => {
          const state = activity[conversation.id]
          const selected = conversation.id === activeConversationId
          return (
            <button
              key={conversation.id}
              type="button"
              className="conversation-card"
              data-selected={selected || undefined}
              onClick={() => {
                startTransition(() => onConversationChange(conversation.id))
              }}
            >
              <Avatar className="conversation-card__avatar" data-tone={conversation.tone}>
                <AvatarFallback>{conversation.initials}</AvatarFallback>
              </Avatar>
              <span className="conversation-card__copy">
                <span className="conversation-card__title">
                  <strong>{conversation.title}</strong>
                  <time>{relativeTime(state?.updatedAt ?? conversation.updatedAt)}</time>
                </span>
                <span className="conversation-card__preview">
                  {state?.preview ?? conversation.summary}
                </span>
                <span className="conversation-card__meta">
                  {conversation.seededMessages} message history
                  {state?.unread ? <b>{state.unread} new</b> : null}
                </span>
              </span>
            </button>
          )
        })}
      </nav>
      <div className="chat-inbox__footer">
        <Avatar className="chat-inbox__user">
          <AvatarFallback>YO</AvatarFallback>
        </Avatar>
        <div>
          <strong>You</strong>
          <span>Demo workspace</span>
        </div>
        <WifiIcon aria-label="Connected" />
      </div>
    </aside>
  )
}

function LoadingTranscript({ conversation }: { conversation: DemoConversation }) {
  return (
    <div className="chat-loading" role="status" aria-live="polite">
      <div className="chat-loading__pulse">
        <LoaderCircleIcon aria-hidden="true" />
      </div>
      <strong>Loading {conversation.seededMessages} messages</strong>
      <span>Replaying {conversation.title} into one complete view…</span>
      <div className="chat-loading__lines" aria-hidden="true">
        <i />
        <i />
        <i />
      </div>
    </div>
  )
}

function Transcript({
  entries,
  clientId,
}: {
  entries: readonly DemoChatEntry[]
  clientId: string
}) {
  return (
    <MessageScrollerProvider autoScroll>
      <MessageScroller className="product-transcript">
        <MessageScrollerViewport>
          <MessageScrollerContent className="product-transcript__content">
            <div className="conversation-date">
              <span>Conversation history</span>
            </div>
            {entries.map((entry, index) => {
              const user = entry.message.role === 'user'
              const followsSameRole = entries[index - 1]?.message.role === entry.message.role
              return (
                <MessageScrollerItem
                  key={entry.message.id}
                  messageId={entry.message.id}
                  scrollAnchor={entry.message.clientId === clientId}
                  className="product-message"
                  data-role={entry.message.role}
                  data-continued={followsSameRole || undefined}
                >
                  {!followsSameRole ? (
                    <Avatar className="product-message__avatar">
                      <AvatarFallback>{user ? 'YO' : 'AI'}</AvatarFallback>
                    </Avatar>
                  ) : (
                    <span className="product-message__avatar-spacer" />
                  )}
                  <div className="product-message__body">
                    {!followsSameRole ? (
                      <header>
                        <strong>{user ? 'You' : entry.message.author}</strong>
                        <time>{messageTime(entry.message.sentAt)}</time>
                      </header>
                    ) : null}
                    <p>{entry.message.body}</p>
                    <span className="product-message__cursor">#{entry.cursor}</span>
                  </div>
                </MessageScrollerItem>
              )
            })}
          </MessageScrollerContent>
        </MessageScrollerViewport>
        <MessageScrollerButton />
      </MessageScroller>
    </MessageScrollerProvider>
  )
}

function Composer({
  conversation,
  disabled,
  onSend,
}: {
  conversation: DemoConversation
  disabled: boolean
  onSend: (body: string) => Promise<void>
}) {
  const [body, setBody] = useState('')
  const [sending, setSending] = useState(false)

  useEffect(() => setBody(''), [conversation.id])

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    const value = body.trim()
    if (!value || disabled || sending) return
    setSending(true)
    try {
      await onSend(value)
      setBody('')
    } finally {
      setSending(false)
    }
  }

  return (
    <form className="product-composer" onSubmit={(event) => void submit(event)}>
      <textarea
        value={body}
        rows={2}
        maxLength={1_200}
        disabled={disabled}
        placeholder={`Message ${conversation.assistant}…`}
        onChange={(event) => setBody(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault()
            event.currentTarget.form?.requestSubmit()
          }
        }}
      />
      <footer>
        <span>
          <SparklesIcon aria-hidden="true" /> Assistant replies are published through NATS
        </span>
        <Button type="submit" size="icon" disabled={disabled || sending || !body.trim()}>
          {sending ? <LoaderCircleIcon className="spin" /> : <SendIcon />}
          <span className="sr-only">Send message</span>
        </Button>
      </footer>
    </form>
  )
}

function PerformancePanel({
  adapter,
  metrics,
  phase,
  onBusyBurst,
}: Pick<PerformanceChatProps, 'adapter' | 'metrics' | 'onBusyBurst' | 'phase'>) {
  const [running, setRunning] = useState(false)
  const values = [
    ['History events', metrics.historyEvents],
    [
      'History ready',
      metrics.historyReadyMs === undefined ? '—' : `${metrics.historyReadyMs.toFixed(1)} ms`,
    ],
    ['State updates', metrics.stateUpdates],
    ['React commits', metrics.reactCommits],
    ['Last UI batch', metrics.lastBatchSize],
    ['Largest UI batch', metrics.largestBatchSize],
  ] as const

  return (
    <aside className="performance-panel" aria-labelledby="performance-title">
      <div className="performance-panel__heading">
        <div>
          <span>LIVE MEASUREMENTS</span>
          <h2 id="performance-title">{adapter === 'effect' ? 'Effect' : 'RxJS'} pipeline</h2>
        </div>
        <GaugeIcon aria-hidden="true" />
      </div>
      <p>These counters measure the same replay, live burst, and React surface in both examples.</p>
      <dl>
        {values.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd className="tabular-nums" data-metric={label.toLowerCase().replaceAll(' ', '-')}>
              {value}
            </dd>
          </div>
        ))}
      </dl>
      <div className="performance-panel__callout">
        <ActivityIcon aria-hidden="true" />
        <div>
          <strong>Busy-room scenario</strong>
          <span>Publish 40 small assistant updates as one realistic live burst.</span>
        </div>
      </div>
      <Button
        type="button"
        variant="outline"
        disabled={phase !== 'live' || running}
        onClick={() => {
          setRunning(true)
          void onBusyBurst().finally(() => setRunning(false))
        }}
      >
        <ChevronsUpIcon data-icon="inline-start" />
        {running ? 'Publishing updates…' : 'Simulate busy room'}
      </Button>
      <div className="performance-panel__legend">
        <span>
          <i data-kind="replay" /> Atomic history
        </span>
        <span>
          <i data-kind="live" /> Frame-batched live state
        </span>
        <span>
          <i data-kind="react" /> React commit boundary
        </span>
      </div>
    </aside>
  )
}

export function PerformanceChat(props: PerformanceChatProps) {
  const conversation =
    props.conversations.find((candidate) => candidate.id === props.activeConversationId) ??
    props.conversations[0]!
  const [showMetrics, setShowMetrics] = useState(true)
  const tabCount = useTabPresence(props.adapter, props.clientId)
  const lastProfiledRevision = useRef(-1)

  return (
    <main className="chat-lab" data-example-mode={props.adapter} data-chat-phase={props.phase}>
      <ConversationList
        conversations={props.conversations}
        activeConversationId={props.activeConversationId}
        activity={props.activity}
        onConversationChange={props.onConversationChange}
      />
      <section className="chat-stage">
        <header className="chat-stage__header">
          <div className="chat-stage__identity">
            <Avatar data-tone={conversation.tone}>
              <AvatarFallback>{conversation.initials}</AvatarFallback>
            </Avatar>
            <div>
              <h1>{conversation.title}</h1>
              <span>
                {props.phase === 'live' ? (
                  <CheckIcon aria-hidden="true" />
                ) : (
                  <LoaderCircleIcon className="spin" />
                )}
                {props.phase === 'live'
                  ? `${conversation.assistant} · caught up`
                  : 'Reassembling history'}
              </span>
            </div>
          </div>
          <div className="chat-stage__actions">
            <Badge variant="outline" className="tab-presence">
              <MessageSquareTextIcon /> {tabCount} {tabCount === 1 ? 'tab' : 'tabs'}
            </Badge>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => window.open(window.location.href, '_blank', 'noopener,noreferrer')}
            >
              <ExternalLinkIcon data-icon="inline-start" />
              Open another tab
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              aria-label={showMetrics ? 'Hide performance panel' : 'Show performance panel'}
              onClick={() => setShowMetrics((visible) => !visible)}
            >
              {showMetrics ? <PanelRightCloseIcon /> : <PanelRightOpenIcon />}
            </Button>
          </div>
        </header>

        {props.notice ? (
          <div className="update-notice" role="status">
            <BellIcon aria-hidden="true" />
            <div>
              <strong>{props.notice.title}</strong>
              <span>{props.notice.body}</span>
            </div>
            {props.notice.conversationId !== props.activeConversationId ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => props.onConversationChange(props.notice!.conversationId)}
              >
                View
              </Button>
            ) : null}
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              aria-label="Dismiss update"
              onClick={props.onDismissNotice}
            >
              <XIcon />
            </Button>
          </div>
        ) : null}

        <Profiler
          id={`${props.adapter}-conversation`}
          onRender={(_id, _phase, actualDuration) => {
            if (lastProfiledRevision.current === props.revision) return
            lastProfiledRevision.current = props.revision
            props.onReactCommit(props.revision, actualDuration)
          }}
        >
          <div className="chat-stage__conversation">
            {props.phase === 'replaying' || props.phase === 'connecting' ? (
              <LoadingTranscript conversation={conversation} />
            ) : props.phase === 'error' ? (
              <div className="chat-error">
                <strong>This conversation could not be loaded.</strong>
                <span>Check the local NATS fixture and reload the example.</span>
              </div>
            ) : (
              <Transcript entries={props.entries} clientId={props.clientId} />
            )}
          </div>
        </Profiler>
        <Composer
          conversation={conversation}
          disabled={props.phase !== 'live'}
          onSend={props.onSend}
        />
      </section>
      {showMetrics ? (
        <PerformancePanel
          adapter={props.adapter}
          metrics={props.metrics}
          phase={props.phase}
          onBusyBurst={props.onBusyBurst}
        />
      ) : null}
    </main>
  )
}
