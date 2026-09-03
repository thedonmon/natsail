import {
  Profiler,
  memo,
  startTransition,
  useEffect,
  useLayoutEffect,
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
import { Alert, AlertAction, AlertDescription, AlertTitle } from '#components/ui/alert'
import { Badge } from '#components/ui/badge'
import { Bubble, BubbleContent } from '#components/ui/bubble'
import { Button } from '#components/ui/button'
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from '#components/ui/empty'
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupText,
  InputGroupTextarea,
  InputGroupInput,
} from '#components/ui/input-group'
import { Marker, MarkerContent } from '#components/ui/marker'
import {
  Message,
  MessageAvatar,
  MessageContent,
  MessageFooter,
  MessageHeader,
} from '#components/ui/message'
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from '#components/ui/message-scroller'
import { Spinner } from '#components/ui/spinner'
import { ToggleGroup, ToggleGroupItem } from '#components/ui/toggle-group'
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
  readonly onBusyBurst: (count: number) => Promise<void>
  readonly onRoomUpdate: () => Promise<void>
  readonly onDismissNotice: () => void
  readonly onReactCommit: (revision: number, duration?: number) => void
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
      <div className="chat-search">
        <InputGroup>
          <InputGroupAddon>
            <SearchIcon aria-hidden="true" />
          </InputGroupAddon>
          <InputGroupInput
            aria-label="Search conversations"
            value={query}
            placeholder="Search conversations"
            onChange={(event) => setQuery(event.target.value)}
          />
        </InputGroup>
      </div>
      <div className="chat-inbox__label">
        <span>Recent</span>
        <Badge variant="outline">{conversations.length}</Badge>
      </div>
      <nav className="conversation-list">
        {visible.length === 0 ? (
          <Empty className="conversation-list__empty">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <SearchIcon />
              </EmptyMedia>
              <EmptyTitle>No conversations found</EmptyTitle>
              <EmptyDescription>Try another title or clear the search.</EmptyDescription>
            </EmptyHeader>
            <EmptyContent>
              <Button type="button" variant="ghost" size="sm" onClick={() => setQuery('')}>
                Clear search
              </Button>
            </EmptyContent>
          </Empty>
        ) : null}
        {visible.map((conversation) => {
          const state = activity[conversation.id]
          const selected = conversation.id === activeConversationId
          return (
            <button
              key={conversation.id}
              type="button"
              className="conversation-card"
              data-selected={selected || undefined}
              aria-current={selected ? 'page' : undefined}
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
                  {conversation.seededMessages.toLocaleString()} message history
                  {state?.unread ? <Badge>{state.unread} new</Badge> : null}
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
        <Badge variant="outline" className="chat-inbox__connection">
          <WifiIcon aria-hidden="true" /> Connected
        </Badge>
      </div>
    </aside>
  )
}

function LoadingTranscript({ conversation }: { conversation: DemoConversation }) {
  return (
    <Empty className="chat-loading" role="status" aria-live="polite">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Spinner />
        </EmptyMedia>
        <EmptyTitle>Loading {conversation.seededMessages.toLocaleString()} messages</EmptyTitle>
        <EmptyDescription>
          {conversation.title} appears after its retained history reaches the live boundary.
        </EmptyDescription>
      </EmptyHeader>
      <EmptyContent>
        <Badge variant="outline">Atomic JetStream replay</Badge>
      </EmptyContent>
    </Empty>
  )
}

const Transcript = memo(function Transcript({
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
            <MessageScrollerItem messageId="conversation-history-marker">
              <Marker variant="separator" className="conversation-date">
                <MarkerContent>Conversation history</MarkerContent>
              </Marker>
            </MessageScrollerItem>
            {entries.map((entry, index) => {
              const user = entry.message.role === 'user'
              const followsSameRole = entries[index - 1]?.message.role === entry.message.role
              return (
                <MessageScrollerItem
                  key={entry.message.id}
                  messageId={entry.message.id}
                  scrollAnchor={entry.message.clientId === clientId}
                  className="product-message-item"
                  data-continued={followsSameRole || undefined}
                >
                  <Message
                    className="product-message"
                    align={user ? 'end' : 'start'}
                    data-role={entry.message.role}
                  >
                    <MessageAvatar
                      className="product-message__avatar"
                      data-empty={followsSameRole || undefined}
                      aria-hidden={followsSameRole || undefined}
                    >
                      {followsSameRole ? null : (
                        <Avatar>
                          <AvatarFallback>{user ? 'YO' : 'AI'}</AvatarFallback>
                        </Avatar>
                      )}
                    </MessageAvatar>
                    <MessageContent className="product-message__body">
                      {!followsSameRole ? (
                        <MessageHeader>
                          <strong>{user ? 'You' : entry.message.author}</strong>
                          <time>{messageTime(entry.message.sentAt)}</time>
                        </MessageHeader>
                      ) : null}
                      <Bubble
                        variant={user ? 'tinted' : 'secondary'}
                        align={user ? 'end' : 'start'}
                      >
                        <BubbleContent>{entry.message.body}</BubbleContent>
                      </Bubble>
                      <MessageFooter>
                        <span className="product-message__cursor">stream #{entry.cursor}</span>
                      </MessageFooter>
                    </MessageContent>
                  </Message>
                </MessageScrollerItem>
              )
            })}
          </MessageScrollerContent>
        </MessageScrollerViewport>
        <MessageScrollerButton />
      </MessageScroller>
    </MessageScrollerProvider>
  )
})

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
    <form className="product-composer-form" onSubmit={(event) => void submit(event)}>
      <InputGroup className="product-composer">
        <InputGroupTextarea
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
        <InputGroupAddon align="block-end" className="product-composer__footer">
          <InputGroupText>
            <SparklesIcon aria-hidden="true" /> Replies travel through NATS
          </InputGroupText>
          <InputGroupButton
            type="submit"
            variant="default"
            size="icon-sm"
            aria-label="Send message"
            disabled={disabled || sending || !body.trim()}
          >
            {sending ? <Spinner /> : <SendIcon />}
          </InputGroupButton>
        </InputGroupAddon>
      </InputGroup>
    </form>
  )
}

function PerformancePanel({
  adapter,
  metrics,
  phase,
  onBusyBurst,
  onRoomUpdate,
  onClose,
}: Pick<PerformanceChatProps, 'adapter' | 'metrics' | 'onBusyBurst' | 'onRoomUpdate' | 'phase'> & {
  onClose: () => void
}) {
  const [running, setRunning] = useState(false)
  const [notifying, setNotifying] = useState(false)
  const [burstSize, setBurstSize] = useState(40)
  const groups = [
    {
      label: 'Replay',
      values: [
        ['History events', metrics.historyEvents],
        [
          'Adapter ready',
          metrics.historyReadyMs === undefined ? '—' : `${metrics.historyReadyMs.toFixed(1)} ms`,
        ],
        [
          'React rendered',
          metrics.historyRenderedMs === undefined
            ? '—'
            : `${metrics.historyRenderedMs.toFixed(1)} ms`,
        ],
      ],
    },
    {
      label: 'NATSail telemetry',
      values: [
        ['Measurements', metrics.telemetryMeasurements],
        [
          'Replay duration',
          metrics.replayTelemetryMs === undefined
            ? '—'
            : `${metrics.replayTelemetryMs.toFixed(1)} ms`,
        ],
        [
          'Last handler',
          metrics.lastHandlerTelemetryMs === undefined
            ? '—'
            : `${metrics.lastHandlerTelemetryMs.toFixed(2)} ms`,
        ],
        [
          'Average publish',
          metrics.averagePublishTelemetryMs === undefined
            ? '—'
            : `${metrics.averagePublishTelemetryMs.toFixed(2)} ms`,
        ],
        ['Buffer signals', metrics.bufferSignals],
      ],
    },
    {
      label: 'Presentation',
      values: [
        ['State updates', metrics.stateUpdates],
        ['React commits', metrics.reactCommits],
        ['Last UI batch', metrics.lastBatchSize],
        ['Largest live batch', metrics.largestBatchSize],
        [
          'Last commit',
          metrics.lastCommitMs === undefined ? '—' : `${metrics.lastCommitMs.toFixed(1)} ms`,
        ],
        [
          'Largest commit',
          metrics.largestCommitMs === undefined ? '—' : `${metrics.largestCommitMs.toFixed(1)} ms`,
        ],
      ],
    },
  ] as const

  return (
    <aside className="performance-panel" aria-labelledby="performance-title">
      <div className="performance-panel__heading">
        <div>
          <Badge variant="secondary">{adapter === 'effect' ? 'Effect' : 'RxJS'}</Badge>
          <h2 id="performance-title">Stream inspector</h2>
        </div>
        <div className="performance-panel__heading-actions">
          <GaugeIcon aria-hidden="true" />
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            className="performance-panel__close"
            aria-label="Close performance panel"
            onClick={onClose}
          >
            <XIcon />
          </Button>
        </div>
      </div>
      <p>
        One path from retained history to the React surface. Exact commit duration requires a
        development or profiling build.
      </p>
      <div className="stream-tape" aria-label="Replay to render path">
        <span data-kind="replay">
          <i />
          Replay
        </span>
        <b aria-hidden="true">→</b>
        <span data-kind="adapter">
          <i />
          {adapter === 'effect' ? 'Effect' : 'RxJS'}
        </span>
        <b aria-hidden="true">→</b>
        <span data-kind="react">
          <i />
          React
        </span>
      </div>
      <div className="performance-panel__metrics">
        {groups.map((group) => (
          <section key={group.label}>
            <h3>{group.label}</h3>
            <dl>
              {group.values.map(([label, value]) => (
                <div key={label}>
                  <dt>{label}</dt>
                  <dd
                    className="tabular-nums"
                    data-metric={label.toLowerCase().replaceAll(' ', '-')}
                  >
                    {value}
                  </dd>
                </div>
              ))}
            </dl>
          </section>
        ))}
      </div>
      <Alert className="performance-panel__notification-test">
        <BellIcon aria-hidden="true" />
        <AlertTitle>Room notification</AlertTitle>
        <AlertDescription>
          Publish one real NATS update to an inactive room. Its banner stays until dismissed.
        </AlertDescription>
      </Alert>
      <Button
        type="button"
        variant="outline"
        disabled={phase !== 'live' || notifying}
        onClick={() => {
          setNotifying(true)
          void onRoomUpdate().finally(() => setNotifying(false))
        }}
      >
        <BellIcon data-icon="inline-start" />
        {notifying ? 'Publishing room update…' : 'Trigger room notification'}
      </Button>
      <Alert className="performance-panel__callout">
        <ActivityIcon aria-hidden="true" />
        <AlertTitle>Busy-room scenario</AlertTitle>
        <AlertDescription>
          Publish compact assistant updates as one bounded live burst.
        </AlertDescription>
      </Alert>
      <ToggleGroup
        className="performance-panel__burst-options"
        aria-label="Live burst size"
        value={[String(burstSize)]}
        variant="outline"
        size="sm"
        spacing={0}
        onValueChange={(values) => {
          const value = Number(values.at(-1))
          if (value === 40 || value === 250 || value === 1_000) setBurstSize(value)
        }}
      >
        {[40, 250, 1_000].map((count) => (
          <ToggleGroupItem
            key={count}
            value={String(count)}
            aria-label={`${count.toLocaleString()} updates`}
          >
            {count.toLocaleString()}
          </ToggleGroupItem>
        ))}
      </ToggleGroup>
      <Button
        type="button"
        variant="outline"
        disabled={phase !== 'live' || running}
        onClick={() => {
          setRunning(true)
          void onBusyBurst(burstSize).finally(() => setRunning(false))
        }}
      >
        <ChevronsUpIcon data-icon="inline-start" />
        {running ? `Publishing ${burstSize.toLocaleString()} updates…` : 'Simulate busy room'}
      </Button>
    </aside>
  )
}

export function PerformanceChat(props: PerformanceChatProps) {
  const conversation =
    props.conversations.find((candidate) => candidate.id === props.activeConversationId) ??
    props.conversations[0]!
  const [showMetrics, setShowMetrics] = useState(true)
  const tabCount = useTabPresence(props.adapter, props.clientId)
  const profiledCommit = useRef<
    { readonly revision: number; readonly duration: number } | undefined
  >(undefined)

  useLayoutEffect(() => {
    const profile = profiledCommit.current
    props.onReactCommit(
      props.revision,
      profile?.revision === props.revision ? profile.duration : undefined
    )
  }, [props.onReactCommit, props.revision])

  return (
    <main
      className="chat-lab"
      data-example-mode={props.adapter}
      data-chat-phase={props.phase}
      data-metrics={showMetrics ? 'open' : 'closed'}
    >
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
                ) : props.phase === 'error' ? (
                  <XIcon aria-hidden="true" />
                ) : (
                  <Spinner />
                )}
                {props.phase === 'live'
                  ? `${conversation.assistant} · caught up`
                  : props.phase === 'error'
                    ? 'Stream unavailable'
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
          <Alert className="update-notice" role="status">
            <BellIcon aria-hidden="true" />
            <AlertTitle>{props.notice.title}</AlertTitle>
            <AlertDescription>{props.notice.body}</AlertDescription>
            <AlertAction>
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
            </AlertAction>
          </Alert>
        ) : null}

        <Profiler
          id={`${props.adapter}-conversation`}
          onRender={(_id, _phase, actualDuration) => {
            profiledCommit.current = { revision: props.revision, duration: actualDuration }
          }}
        >
          <div className="chat-stage__conversation">
            {props.phase === 'replaying' || props.phase === 'connecting' ? (
              <LoadingTranscript conversation={conversation} />
            ) : props.phase === 'error' ? (
              <div className="chat-error">
                <Alert variant="destructive">
                  <AlertTitle>This conversation could not be loaded.</AlertTitle>
                  <AlertDescription>
                    Check the local NATS fixture, then reload the example.
                  </AlertDescription>
                </Alert>
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
          onRoomUpdate={props.onRoomUpdate}
          onClose={() => setShowMetrics(false)}
        />
      ) : null}
    </main>
  )
}
