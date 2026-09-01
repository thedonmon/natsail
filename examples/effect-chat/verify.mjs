import { verifyChatLab } from '../verify-chat-lab.mjs'

await verifyChatLab({
  adapter: 'effect',
  baseUrl: 'http://127.0.0.1:4178',
  devScript: 'examples/effect-chat/dev.mjs',
  screenshot: '/tmp/natsail-effect-chat-lab.png',
})
