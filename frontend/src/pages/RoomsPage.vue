<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { createRoom, listRooms, type Room, type RoomFilter } from '../api/rooms'
import { ApiError } from '../api/http'
import RoomDetailPage from './RoomDetailPage.vue'

const props = defineProps<{ token: string; userId: string }>()
const emit = defineEmits<{ expired: [] }>()
const title = ref('')
const filter = ref<RoomFilter>({ status: 'OPEN', owner: '' })
const rooms = ref<Room[]>([])
const nextCursor = ref<string | null>(null)
const selectedId = ref<string>()
const loading = ref(false)
const creating = ref(false)
const listMessage = ref('')
const createMessage = ref('')
let listRequest: AbortController | undefined
let createRequest: AbortController | undefined

function errorMessage(error: unknown): string {
  if (error instanceof ApiError && error.status === 401) emit('expired')
  return error instanceof Error ? error.message : '操作失败，请稍后重试。'
}

async function loadRooms(more = false) {
  listRequest?.abort()
  const attempt = new AbortController()
  listRequest = attempt
  loading.value = true
  listMessage.value = ''
  if (!more) {
    rooms.value = []
    nextCursor.value = null
  }
  try {
    const page = await listRooms(props.token, filter.value, more ? nextCursor.value : null, attempt.signal)
    if (attempt.signal.aborted) return
    rooms.value = more ? [...rooms.value, ...page.items] : page.items
    nextCursor.value = page.next_cursor
  } catch (error) {
    if (!attempt.signal.aborted) listMessage.value = errorMessage(error)
  } finally {
    if (listRequest === attempt) {
      loading.value = false
      listRequest = undefined
    }
  }
}

async function submitRoom() {
  if (creating.value) return
  const value = title.value.trim()
  if (Array.from(value).length < 1 || Array.from(value).length > 80) {
    createMessage.value = '标题去除首尾空白后须为 1–80 个字符。'
    return
  }
  const attempt = new AbortController()
  createRequest = attempt
  creating.value = true
  createMessage.value = ''
  try {
    const room = await createRoom(props.token, value, attempt.signal)
    if (attempt.signal.aborted) return
    title.value = ''
    listRequest?.abort()
    selectedId.value = room.id
  } catch (error) {
    if (attempt.signal.aborted) return
    createMessage.value = errorMessage(error)
    if (!(error instanceof ApiError) || error.status >= 500) {
      createMessage.value += ' 创建结果尚未确认，请先选择“我的房间”并刷新列表，避免重复创建。'
    }
  } finally {
    creating.value = false
    createRequest = undefined
  }
}

function backToList() {
  selectedId.value = undefined
  void loadRooms()
}

onMounted(() => { void loadRooms() })
onBeforeUnmount(() => {
  listRequest?.abort()
  createRequest?.abort()
})
</script>

<template>
  <RoomDetailPage v-if="selectedId" :key="selectedId" :room-id="selectedId" :token="token" :user-id="userId"
    @back="backToList" @expired="emit('expired')" />
  <section v-else aria-labelledby="rooms-title">
    <div class="account-heading">
      <div><span class="eyebrow">LIVE ROOMS</span><h1 id="rooms-title">直播间</h1></div>
      <p class="field-help">创建你的房间，或查看大家的房间。</p>
    </div>
    <form class="account-card room-create" :aria-busy="creating" @submit.prevent="submitRoom">
      <h2>创建房间</h2>
      <label for="room-title">房间标题</label>
      <div class="room-create-fields">
        <input id="room-title" v-model="title" placeholder="给房间起个名字" :disabled="creating" required aria-describedby="room-title-help" />
        <button class="submit-button" type="submit" :disabled="creating">{{ creating ? '正在创建…' : '创建房间' }}</button>
      </div>
      <p id="room-title-help" class="field-help">1–80 个字符，所有已登录用户都可以查看。</p>
      <p v-if="createMessage" class="form-message" data-kind="error" role="alert">{{ createMessage }}</p>
    </form>
    <div class="room-filters">
      <div><label for="room-status">房间状态</label>
        <select id="room-status" v-model="filter.status" :disabled="creating" @change="loadRooms()">
          <option value="OPEN">开放中</option><option value="CLOSED">已关闭</option>
        </select>
      </div>
      <div><label for="room-owner">查看范围</label>
        <select id="room-owner" v-model="filter.owner" :disabled="creating" @change="loadRooms()">
          <option value="">全部房间</option><option value="me">我的房间</option>
        </select>
      </div>
      <button class="retry-button" type="button" :disabled="loading || creating" @click="loadRooms()">刷新列表</button>
    </div>
    <p v-if="listMessage" class="form-message" data-kind="error" role="alert">{{ listMessage }}</p>
    <p v-if="loading" role="status">正在读取房间…</p>
    <p v-else-if="rooms.length === 0 && !listMessage" class="empty-state">当前筛选条件下还没有房间。</p>
    <ul class="room-grid" aria-label="房间列表">
      <li v-for="room in rooms" :key="room.id" class="room-card">
        <span class="room-badge" :data-status="room.status">{{ room.status === 'OPEN' ? '开放中' : '已关闭' }}</span>
        <h2>{{ room.title }}</h2>
        <p class="field-help">房间 {{ room.id }} · {{ room.owner_id === userId ? '我创建的' : `房主 ${room.owner_id}` }}</p>
        <button class="retry-button" type="button" :disabled="creating" @click="selectedId = room.id">查看房间</button>
      </li>
    </ul>
    <button v-if="nextCursor" class="retry-button room-more" type="button" :disabled="loading || creating" @click="loadRooms(true)">加载更多</button>
  </section>
</template>
