<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import type { DanmakuMessage } from '../api/danmaku'
import cover from '../assets/live-cover.png'

const props = defineProps<{ open: boolean; messages: DanmakuMessage[] }>()
const stage = ref<HTMLElement>()
const screenEnabled = ref(true)
const fullscreen = ref(false)
const fullscreenNotice = ref('')
const flying = ref<{ id: string; content: string; lane: number }[]>([])
const timers = new Map<string, ReturnType<typeof setTimeout>>()
const seen = new Set<string>()

function clearFlying() {
  for (const timer of timers.values()) clearTimeout(timer)
  timers.clear()
  flying.value = []
}
function toggleScreen() {
  screenEnabled.value = !screenEnabled.value
  if (!screenEnabled.value) clearFlying()
}
watch(() => props.messages, messages => {
  for (const message of messages) {
    if (seen.has(message.event_id)) continue
    seen.add(message.event_id)
    if (seen.size > 96) seen.delete(seen.values().next().value!)
    if (!props.open || !screenEnabled.value || document.hidden || window.matchMedia('(prefers-reduced-motion: reduce)').matches) continue
    // 屏幕装饰最多同时五条，不排队积压；完整内容仍保留在右侧聊天区。
    const lane = [0, 1, 2, 3, 4].find(index => !flying.value.some(item => item.lane === index))
    if (lane === undefined) continue
    flying.value = [...flying.value, { id: message.event_id, content: message.content.replace(/\s+/g, ' '), lane }]
    timers.set(message.event_id, setTimeout(() => {
      flying.value = flying.value.filter(item => item.id !== message.event_id)
      timers.delete(message.event_id)
    }, 8000))
  }
}, { immediate: true })
watch(() => props.open, open => { if (!open) clearFlying() })
async function toggleFullscreen() {
  fullscreenNotice.value = ''
  try {
    if (document.fullscreenElement === stage.value) await document.exitFullscreen()
    else if (stage.value?.requestFullscreen) await stage.value.requestFullscreen()
    else fullscreenNotice.value = '当前浏览器暂不支持全屏。'
  } catch { fullscreenNotice.value = '暂时无法进入全屏，请稍后再试。' }
}
function syncFullscreen() { fullscreen.value = document.fullscreenElement === stage.value }
onMounted(() => { document.addEventListener('fullscreenchange', syncFullscreen) })
onBeforeUnmount(() => {
  clearFlying()
  document.removeEventListener('fullscreenchange', syncFullscreen)
})
</script>

<template>
  <section ref="stage" class="live-stage" aria-label="直播演示画面">
    <div class="stage-screen">
      <img class="stage-cover" :src="cover" alt="晨光映照雪山与湖泊的直播演示封面" fetchpriority="high" />
      <div class="stage-shade" />
      <span class="stage-live" :data-open="open"><span />{{ open ? 'LIVE' : '已结束' }}</span>
      <span class="demo-label">演示画面</span>
      <div class="screen-messages" aria-hidden="true"><span v-for="item in flying" :key="item.id" class="screen-message" :style="{ top: (12 + item.lane * 14) + '%' }">{{ item.content }}</span></div>
      <div v-if="!open" class="stage-ended"><span class="ended-moon" aria-hidden="true">☾</span><h2>本次直播已结束</h2><p>期待下一次相聚</p></div>
      <div v-else class="stage-signature"><span class="signature-line" />LiveStream<span class="signature-note">一起分享此刻</span></div>
    </div>
    <footer class="stage-toolbar">
      <span class="stage-caption"><span class="caption-dot" />{{ open ? '正在直播' : '直播结束' }}<span class="caption-divider">/</span><span>演示画面</span></span>
      <div class="stage-buttons">
        <button type="button" :aria-pressed="screenEnabled" aria-label="屏幕弹幕" @click="toggleScreen"><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true"><rect x="2.5" y="3.5" width="15" height="12" rx="2" /><path d="M6 7h8M6 10h5m-6 7h10" /></svg><span>弹幕</span><span class="toggle-track" :data-on="screenEnabled"><span /></span></button>
        <button type="button" :aria-label="fullscreen ? '退出全屏' : '进入全屏'" :title="fullscreen ? '退出全屏' : '全屏'" @click="toggleFullscreen"><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M7 3H3v4m10-4h4v4M3 13v4h4m10-4v4h-4" /></svg></button>
      </div>
    </footer>
    <p v-if="fullscreenNotice" class="fullscreen-notice" role="status">{{ fullscreenNotice }}</p>
  </section>
</template>

<style scoped>
.live-stage { overflow: hidden; border-radius: 16px; background: #171c27; }
.stage-screen { position: relative; aspect-ratio: 16 / 9; overflow: hidden; container-type: inline-size; }
.stage-cover { display: block; width: 100%; height: 100%; object-fit: cover; }
.stage-shade { position: absolute; inset: 0; pointer-events: none; background: linear-gradient(180deg, #14203135, transparent 28%, transparent 65%, #0b162b70); }
.stage-live, .demo-label { position: absolute; top: 19px; padding: 4px 9px; border-radius: 5px; color: white; font-size: 10px; font-weight: 600; letter-spacing: 1px; backdrop-filter: blur(8px); }
.stage-live { left: 19px; display: flex; align-items: center; gap: 5px; background: #ef5d71d9; }
.stage-live > span { width: 4px; height: 4px; border-radius: 50%; background: white; }
.stage-live[data-open="false"] { background: #535b70cd; }
.demo-label { right: 19px; background: #14203152; font-weight: 400; letter-spacing: .4px; }
.screen-messages { position: absolute; inset: 0; pointer-events: none; overflow: hidden; }
.screen-message { position: absolute; left: 0; white-space: nowrap; font-size: clamp(13px, 1.3vw, 19px); font-weight: 600; color: #fff; text-shadow: 0 1px 3px #000b, 1px 0 2px #0008; animation: cross-screen 8s linear forwards; }
@keyframes cross-screen { from { transform: translateX(100cqw); } to { transform: translateX(-100%); } }
.stage-signature { position: absolute; bottom: 23px; left: 25px; display: flex; align-items: center; gap: 10px; color: #ffffffeb; font-size: 13px; font-weight: 600; letter-spacing: .4px; }
.signature-line { display: block; width: 3px; height: 14px; background: #ffffffb0; border-radius: 2px; }
.signature-note { font-size: 10px; font-weight: 400; color: #ffffffb3; margin-left: 3px; }
.stage-toolbar { display: flex; align-items: center; justify-content: space-between; min-height: 49px; padding: 0 19px; gap: 12px; background: #1c2130; color: #a4aabc; }
.stage-caption { display: flex; align-items: center; gap: 7px; font-size: 11px; white-space: nowrap; }
.caption-dot { width: 5px; height: 5px; background: #7e83a4; border-radius: 50%; }
.caption-divider { margin: 0 4px; color: #464d64; }
.stage-buttons { display: flex; align-items: center; gap: 17px; }
.stage-buttons button { display: flex; align-items: center; gap: 6px; padding: 6px 0; color: #c2c5d2; border: 0; background: transparent; font-size: 11px; }
.stage-buttons button:hover { color: white; }
.stage-buttons svg { width: 18px; height: 18px; }
.toggle-track { display: block; width: 24px; height: 13px; padding: 2px; border-radius: 9px; background: #4e5269; }
.toggle-track > span { display: block; width: 9px; height: 9px; background: #fff; border-radius: 50%; }
.toggle-track[data-on="true"] { background: #8a7cf2; }
.toggle-track[data-on="true"] > span { margin-left: 11px; }
.stage-ended { position: absolute; inset: 0; display: flex; flex-direction: column; justify-content: center; align-items: center; background: #10182c94; color: white; backdrop-filter: blur(3px); }
.ended-moon { font-size: 45px; line-height: 1.3; color: #d7d1ff; }
.stage-ended h2 { font-size: 20px; margin: 12px 0 4px; }
.stage-ended p { font-size: 12px; margin: 0; color: #e0def0c4; }
.fullscreen-notice { margin: 0; padding: 9px 18px; font-size: 12px; color: #e9dcbd; }
.live-stage:fullscreen { display: flex; flex-direction: column; border-radius: 0; width: 100%; height: 100%; }
.live-stage:fullscreen .stage-screen { flex: 1; min-height: 0; aspect-ratio: auto; }
.live-stage:fullscreen .stage-cover { object-fit: contain; }
@media (prefers-reduced-motion: reduce) { .screen-message { display: none; animation: none; } }
@media (max-width: 540px) { .stage-toolbar { padding: 0 12px; } .stage-caption { font-size: 10px; gap: 5px; } .stage-buttons { gap: 10px; } .signature-note { display: none; } }
</style>
