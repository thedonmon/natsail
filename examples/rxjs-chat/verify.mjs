import { verifyChatLab } from '../verify-chat-lab.mjs'

await verifyChatLab({
  adapter: 'rxjs',
  baseUrl: 'http://127.0.0.1:4177',
  devScript: 'examples/rxjs-chat/dev.mjs',
  screenshot: '/tmp/natsail-rxjs-chat-lab.png',
})
