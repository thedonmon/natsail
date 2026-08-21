import { queryOptions } from '@tanstack/react-query'

import {
  isChatMessage,
  rooms,
  type TimelineEntry,
  type TimelineState,
} from '@natsail/example-chat-ui'

interface DataFrame {
  type: 'data'
  value: string
  cursor: number
  duplicate: boolean
}

export interface HistoryResponse {
  prototype: true
  after: number
  retainedLimit: number
  retainedFrom?: number
  retainedThrough?: number
  complete: boolean
  frames: DataFrame[]
}

export const timelineKey = (tenant: string) => ['prototype-chat', tenant, 'timeline'] as const

export const decodeFrame = (frame: DataFrame): TimelineEntry | undefined => {
  try {
    const value: unknown = JSON.parse(frame.value)
    if (!isChatMessage(value)) return undefined
    return {
      message: value,
      cursor: frame.cursor,
      delivery: 'applied',
    }
  } catch {
    return undefined
  }
}

export const fetchHistory = async (tenant: string, after: number): Promise<HistoryResponse> => {
  const response = await fetch(
    `/gateway/${tenant}/history?token=prototype-only&after=${encodeURIComponent(after)}`
  )
  if (!response.ok) {
    throw new Error(`Gateway history returned ${response.status}`)
  }
  return response.json() as Promise<HistoryResponse>
}

const loadTimeline = async (tenant: string): Promise<TimelineState> => {
  const history = await fetchHistory(tenant, 0)
  const messages = history.frames.flatMap((frame) => {
    const entry = decodeFrame(frame)
    return entry ? [entry] : []
  })

  return {
    messages,
    cursor: history.retainedThrough ?? 0,
    phase: history.complete ? 'connecting' : 'gap',
    catchUpCount: 0,
    ...(history.retainedFrom === undefined ? {} : { retainedFrom: history.retainedFrom }),
    ...(history.retainedThrough === undefined ? {} : { retainedThrough: history.retainedThrough }),
    ...(history.complete
      ? {}
      : { diagnostic: 'The beginning of the retained window has expired.' }),
  }
}

export const gatewayQueries = {
  rooms: () =>
    queryOptions({
      queryKey: ['prototype-chat', 'rooms'],
      queryFn: async () => rooms,
      staleTime: Number.POSITIVE_INFINITY,
      gcTime: Number.POSITIVE_INFINITY,
    }),
  timeline: (tenant: string) =>
    queryOptions({
      queryKey: timelineKey(tenant),
      queryFn: () => loadTimeline(tenant),
      staleTime: Number.POSITIVE_INFINITY,
      gcTime: Number.POSITIVE_INFINITY,
    }),
}
