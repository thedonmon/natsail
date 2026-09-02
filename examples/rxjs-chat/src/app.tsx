import { useSyncExternalStore } from 'react'

import { demoConversations, PerformanceChat } from '@natsail/example-chat-ui'
import { RxjsChatController } from './chat-feed'
import { runtime, sessions } from './runtime'

const clientId = `rxjs-tab-${crypto.randomUUID().slice(0, 8)}`
const controller = new RxjsChatController(runtime, sessions, clientId)

export const closeChatController = (): Promise<void> => controller.close()

export function App() {
  const state = useSyncExternalStore(
    controller.subscribe,
    controller.getSnapshot,
    controller.getSnapshot
  )

  return (
    <PerformanceChat
      adapter="rxjs"
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
