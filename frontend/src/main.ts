import { createApp } from 'vue'
import App from './App.vue'
import './style.css'

// 将根组件挂载到 index.html 的 #app 容器。
const app = createApp(App)
app.mount('#app')
