import { useSyncExternalStore } from 'react'

import { demoConversations, PerformanceChat } from '@natsail/example-chat-ui'
import { EffectChatController } from './chat-controller'
import { natsail } from './runtime'

const clientId = `effect-tab-${crypto.randomUUID().slice(0, 8)}`
const controller = new EffectChatController(natsail, clientId)

export const closeChatController = (): Promise<void> => controller.close()

export function App() {
  const state = useSyncExternalStore(
    controller.subscribe,
    controller.getSnapshot,
    controller.getSnapshot
  )

  return (
    <PerformanceChat
      adapter="effect"
      conversations={demoConversations}
      activeConversationId={state.activeConversationId}
      activity={state.activity}
      entries={state.entries}
      phase={state.phase}
      metrics={state.metrics}
      revision={state.revision}
      clientId={clientId}
      {...(state.notice === undefined ? {} : { notice: state.notice })}
      onConversationChange={controller.selectConversation}
      onSend={controller.send}
      onBusyBurst={controller.busyBurst}
      onRoomUpdate={controller.roomUpdate}
      onDismissNotice={controller.dismissNotice}
      onReactCommit={controller.recordReactCommit}
    />
  )
}
