import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react'
import {
  CheckIcon,
  CircleDotDashedIcon,
  ExternalLinkIcon,
  RadioIcon,
  RotateCcwIcon,
  SendIcon,
  TriangleAlertIcon,
} from 'lucide-react'

import { Alert, AlertDescription, AlertTitle } from '#components/ui/alert'
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
import { Field, FieldLabel } from '#components/ui/field'
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupTextarea,
} from '#components/ui/input-group'
import { Message, MessageContent, MessageFooter, MessageHeader } from '#components/ui/message'
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from '#components/ui/message-scroller'
import { ToggleGroup, ToggleGroupItem } from '#components/ui/toggle-group'
import { phaseLabel, type Room, type TimelineEntry, type TimelineState } from './model'

export type ExampleMode = 'core' | 'gateway' | 'rxjs'

export interface WorkspaceProps {
  mode: ExampleMode
  rooms: Room[]
  room: Room
  timeline: TimelineState
  clientId: string
  author: string
  applicationSubtitle: string
  architectureDescription: string
  topologyLabel: string
  upstreamLabel: string
  sourceStarts?: number
  connectionAction?: {
    label: string
    onClick: () => void
    disabled?: boolean
  }
  onRoomChange: (roomId: string) => void
  onSend: (body: string) => Promise<void>
}

type ProofStatus = 'pass' | 'pending' | 'blocked'

interface ProofStep {
  title: string
  instruction: string
  result: string
  status: ProofStatus
  action?: ReactNode
}

const formatTime = (value: string): string =>
  new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value))

function ConnectionBadge({ timeline }: { timeline: TimelineState }) {
  const live = timeline.phase === 'live'
  return (
    <Badge
      variant={live ? 'secondary' : timeline.phase === 'error' ? 'destructive' : 'outline'}
      data-gateway-phase={timeline.phase}
    >
      <RadioIcon data-icon="inline-start" />
      {phaseLabel(timeline.phase)}
    </Badge>
  )
}

function ProofStatusIcon({ status }: { status: ProofStatus }) {
  if (status === 'pass') return <CheckIcon aria-hidden="true" />
  if (status === 'blocked') return <TriangleAlertIcon aria-hidden="true" />
  return <CircleDotDashedIcon aria-hidden="true" />
}

function ProofRail({ steps }: { steps: ProofStep[] }) {
  return (
    <section className="proof-rail" aria-labelledby="proof-title">
      <div className="section-heading">
        <span>TEST PLAN</span>
        <h2 id="proof-title" className="text-balance">
          Three checks. One visible result.
        </h2>
        <p className="text-pretty">
          Work top to bottom. A green receipt means the browser observed the behavior itself.
        </p>
      </div>
      <ol className="proof-steps">
        {steps.map((step, index) => (
          <li key={step.title} data-proof-status={step.status}>
            <div className="proof-step__marker">
              <ProofStatusIcon status={step.status} />
              <span>{index + 1}</span>
            </div>
            <div className="proof-step__copy">
              <div className="proof-step__title">
                <h3>{step.title}</h3>
                <Badge variant={step.status === 'pass' ? 'secondary' : 'outline'}>
                  {step.status === 'pass'
                    ? 'Observed'
                    : step.status === 'blocked'
                      ? 'Blocked'
                      : 'Next'}
                </Badge>
              </div>
              <p>{step.instruction}</p>
              <output>{step.result}</output>
              {step.action ? <div className="proof-step__action">{step.action}</div> : null}
            </div>
          </li>
        ))}
      </ol>
    </section>
  )
}

function RoomPicker({
  rooms,
  room,
  onRoomChange,
}: Pick<WorkspaceProps, 'rooms' | 'room' | 'onRoomChange'>) {
  return (
    <Field className="room-picker">
      <FieldLabel id="room-picker-label">Room under test</FieldLabel>
      <ToggleGroup
        aria-labelledby="room-picker-label"
        value={[room.id]}
        variant="outline"
        size="sm"
        onValueChange={(values) => {
          const value = values.at(-1)
          if (value) onRoomChange(value)
        }}
      >
        {rooms.map((candidate) => (
          <ToggleGroupItem key={candidate.id} value={candidate.id} aria-label={candidate.label}>
            <span>{candidate.signal}</span>
            {candidate.label}
          </ToggleGroupItem>
        ))}
      </ToggleGroup>
    </Field>
  )
}

function Transcript({
  entries,
  room,
  clientId,
}: {
  entries: TimelineEntry[]
  room: Room
  clientId: string
}) {
  return (
    <MessageScrollerProvider autoScroll>
      <MessageScroller className="transcript">
        <MessageScrollerViewport>
          <MessageScrollerContent className="transcript__content">
            {entries.length === 0 ? (
              <Empty className="transcript__empty">
                <EmptyHeader>
                  <EmptyMedia variant="icon">
                    <RadioIcon />
                  </EmptyMedia>
                  <EmptyTitle>No deliveries in {room.label}</EmptyTitle>
                  <EmptyDescription>
                    Send the prepared probe below. Its round trip will appear here with a receipt.
                  </EmptyDescription>
                </EmptyHeader>
                <EmptyContent>
                  <Badge variant="outline">Subject {room.signal}</Badge>
                </EmptyContent>
              </Empty>
            ) : (
              entries.map((entry) => {
                const ownMessage = entry.message.clientId === clientId
                return (
                  <MessageScrollerItem
                    key={entry.message.id}
                    messageId={entry.message.id}
                    scrollAnchor={ownMessage}
                  >
                    <Message align={ownMessage ? 'end' : 'start'}>
                      <MessageContent>
                        <MessageHeader>
                          {entry.message.author} · {formatTime(entry.message.sentAt)}
                        </MessageHeader>
                        <Bubble
                          align={ownMessage ? 'end' : 'start'}
                          variant={
                            entry.delivery === 'failed'
                              ? 'destructive'
                              : ownMessage
                                ? 'default'
                                : 'outline'
                          }
                        >
                          <BubbleContent>{entry.message.body}</BubbleContent>
                        </Bubble>
                        <MessageFooter>
                          {entry.delivery === 'applied'
                            ? `Observed at cursor ${entry.cursor ?? 'live'}`
                            : entry.delivery}
                        </MessageFooter>
                      </MessageContent>
                    </Message>
                  </MessageScrollerItem>
                )
              })
            )}
          </MessageScrollerContent>
        </MessageScrollerViewport>
        <MessageScrollerButton />
      </MessageScroller>
    </MessageScrollerProvider>
  )
}

function ProbeComposer({
  room,
  phase,
  onSend,
}: {
  room: Room
  phase: TimelineState['phase']
  onSend: (body: string) => Promise<void>
}) {
  const [body, setBody] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string>()
  const live = phase === 'live'

  useEffect(() => {
    setBody('')
    setError(undefined)
  }, [room.signal])

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    const value = body.trim()
    if (!value || !live || submitting) return
    setSubmitting(true)
    setError(undefined)
    try {
      await onSend(value)
      setBody('')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form className="probe-composer" onSubmit={(event) => void submit(event)}>
      <Field data-disabled={!live || undefined} data-invalid={error ? true : undefined}>
        <FieldLabel htmlFor="probe-body" className="sr-only">
          Message {room.label}
        </FieldLabel>
        <InputGroup>
          <InputGroupTextarea
            id="probe-body"
            name="message"
            rows={2}
            value={body}
            disabled={!live}
            aria-invalid={error ? true : undefined}
            maxLength={700}
            placeholder={`Message ${room.label}`}
            onChange={(event) => setBody(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault()
                event.currentTarget.form?.requestSubmit()
              }
            }}
          />
          <InputGroupAddon align="block-end">
            <span>{live ? `Ready on ${room.signal}` : `Waiting while ${phase}`}</span>
            <InputGroupButton
              type="submit"
              variant="default"
              size="sm"
              disabled={!live || !body.trim() || submitting}
            >
              <SendIcon data-icon="inline-start" />
              {submitting ? 'Sending…' : 'Send message'}
            </InputGroupButton>
          </InputGroupAddon>
        </InputGroup>
        {error ? <p className="probe-composer__error">{error}</p> : null}
      </Field>
    </form>
  )
}

function EvidencePanel({ props }: { props: WorkspaceProps }) {
  const { timeline } = props
  const receipts = [
    ['Browser cursor', timeline.cursor || '—'],
    [`${props.upstreamLabel} cursor`, timeline.gatewayCursor ?? '—'],
    ['Catch-up runs', timeline.catchUpCount],
    ['Deliveries seen', timeline.messages.length],
    ...(props.sourceStarts === undefined ? [] : [['Observable source starts', props.sourceStarts]]),
  ] as const

  return (
    <aside className="evidence-panel" aria-labelledby="evidence-title">
      <div className="section-heading">
        <span>LIVE EVIDENCE</span>
        <h2 id="evidence-title">Receipts from this tab</h2>
      </div>
      <dl>
        {receipts.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd
              className="tabular-nums"
              data-catchups={label === 'Catch-up runs' ? value : undefined}
              data-deliveries={label === 'Deliveries seen' ? value : undefined}
              data-source-starts={label === 'Observable source starts' ? value : undefined}
            >
              {value}
            </dd>
          </div>
        ))}
      </dl>
      {timeline.diagnostic ? (
        <Alert
          variant={
            timeline.phase === 'error' || timeline.phase === 'gap' ? 'destructive' : 'default'
          }
        >
          <TriangleAlertIcon />
          <AlertTitle>Transport diagnostic</AlertTitle>
          <AlertDescription>{timeline.diagnostic}</AlertDescription>
        </Alert>
      ) : null}
      <details className="architecture-details">
        <summary>How this example is wired</summary>
        <p>{props.architectureDescription}</p>
        <dl>
          <div>
            <dt>Topology</dt>
            <dd>{props.topologyLabel}</dd>
          </div>
          <div>
            <dt>Client</dt>
            <dd>{props.clientId}</dd>
          </div>
          <div>
            <dt>Operator</dt>
            <dd>{props.author}</dd>
          </div>
        </dl>
      </details>
    </aside>
  )
}

export function Workspace(props: WorkspaceProps) {
  const entries = props.timeline.messages.filter((entry) => entry.message.roomId === props.room.id)
  const ownDelivery = props.timeline.messages.find(
    (entry) => entry.message.clientId === props.clientId && entry.delivery === 'applied'
  )
  const observedRooms = useMemo(
    () => new Set(props.timeline.messages.map((entry) => entry.message.roomId)).size,
    [props.timeline.messages]
  )
  const live = props.timeline.phase === 'live'

  const thirdStep: ProofStep =
    props.mode === 'gateway'
      ? {
          title: 'Recover a missed delivery',
          instruction:
            'Open a second tab. Disconnect this tab, publish from the other tab, then reconnect here.',
          result:
            props.timeline.catchUpCount > 0
              ? `${props.timeline.catchUpCount} retained catch-up run observed.`
              : 'Waiting for a disconnect → publish elsewhere → reconnect cycle.',
          status: props.timeline.catchUpCount > 0 ? 'pass' : 'pending',
          action: (
            <div className="button-row">
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => window.open(window.location.href, '_blank', 'noopener,noreferrer')}
              >
                <ExternalLinkIcon data-icon="inline-start" />
                Open second tab
              </Button>
              {props.connectionAction ? (
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={props.connectionAction.disabled}
                  onClick={props.connectionAction.onClick}
                >
                  <RotateCcwIcon data-icon="inline-start" />
                  {props.connectionAction.label}
                </Button>
              ) : null}
            </div>
          ),
        }
      : props.mode === 'rxjs'
        ? {
            title: 'Recover retained deliveries',
            instruction:
              'Pause the ordered consumer. The app publishes three messages before it resumes from its checkpoint.',
            result:
              props.timeline.catchUpCount > 0
                ? `${props.timeline.catchUpCount} retained recovery run observed.`
                : 'Waiting for one pause, publish, and resume cycle.',
            status: props.timeline.catchUpCount > 0 ? 'pass' : 'pending',
            action: props.connectionAction ? (
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={props.connectionAction.disabled}
                onClick={props.connectionAction.onClick}
              >
                <RotateCcwIcon data-icon="inline-start" />
                {props.connectionAction.label}
              </Button>
            ) : null,
          }
        : {
            title: 'Verify one shared room feed',
            instruction:
              'Switch rooms above the transcript. Seeded deliveries all came through one reducer hook.',
            result:
              observedRooms >= 2
                ? `${observedRooms} rooms folded into one subscription snapshot.`
                : 'Waiting for the seeded multi-room deliveries.',
            status: observedRooms >= 2 ? 'pass' : 'pending',
          }

  const steps: ProofStep[] = [
    {
      title: 'Establish the transport',
      instruction: 'The example connects automatically. No click is required.',
      result: live
        ? 'The browser reports a live transport.'
        : `Current phase: ${phaseLabel(props.timeline.phase)}.`,
      status: live ? 'pass' : props.timeline.phase === 'error' ? 'blocked' : 'pending',
    },
    {
      title: 'Observe a publish round trip',
      instruction:
        'Send a message below. Wait for the same message to return with a cursor receipt.',
      result: ownDelivery
        ? `Message returned as cursor ${ownDelivery.cursor ?? 'live'}.`
        : 'No message from this browser has completed the round trip yet.',
      status: ownDelivery ? 'pass' : live ? 'pending' : 'blocked',
    },
    thirdStep,
  ]

  return (
    <main className="workspace" data-example-mode={props.mode}>
      <header className="lab-header">
        <div className="lab-brand">
          <span className="lab-brand__mark" aria-hidden="true">
            N/
          </span>
          <div>
            <span>NATSail example</span>
            <strong>{props.applicationSubtitle}</strong>
          </div>
        </div>
        <div className="lab-header__purpose">
          <span>WHAT THIS PROVES</span>
          <p className="text-pretty">
            {props.mode === 'gateway'
              ? 'One browser transport can publish live, miss data, and catch up from retained history.'
              : props.mode === 'rxjs'
                ? 'RxJS projections share one resumable JetStream feed and preserve every room delivery.'
                : 'The React primitive can share one live NATS subscription and reduce every delivery safely.'}
          </p>
        </div>
        <ConnectionBadge timeline={props.timeline} />
      </header>

      <div className="lab-grid">
        <ProofRail steps={steps} />
        <section className="conversation-bench" aria-labelledby="room-title">
          <div className="conversation-bench__header">
            <div>
              <span>ROOM WORKBENCH</span>
              <h1 id="room-title" className="text-balance">
                {props.room.label}
              </h1>
              <p className="text-pretty">{props.room.purpose}</p>
            </div>
            <Badge variant="outline">{entries.length} in view</Badge>
          </div>
          <RoomPicker rooms={props.rooms} room={props.room} onRoomChange={props.onRoomChange} />
          <Transcript entries={entries} room={props.room} clientId={props.clientId} />
          <ProbeComposer room={props.room} phase={props.timeline.phase} onSend={props.onSend} />
        </section>
        <EvidencePanel props={props} />
      </div>
    </main>
  )
}
