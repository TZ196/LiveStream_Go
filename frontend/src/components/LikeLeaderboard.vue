<script setup lang="ts">
import type { BoardRow } from '../leaderboard'

defineProps<{ rows: BoardRow[] | null; mine: string | null; onBoard: boolean; status: string; userId: string }>()
function initial(name: string) { return Array.from(name)[0]?.toUpperCase() ?? '友' }
</script>

<template>
  <details class="like-board" open>
    <summary>
      <div class="board-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="M8 3h8v6a4 4 0 0 1-8 0V3Zm0 2H4v2a4 4 0 0 0 4 4m8-6h4v2a4 4 0 0 1-4 4M12 13v6m-4 2h8m-6-2h4" /></svg><h2>本场点赞榜</h2><span class="top-label">TOP 100</span></div>
      <svg class="collapse-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg>
    </summary>
    <div class="board-body">
      <p v-if="status" class="board-status" role="status">{{ status }}</p>
      <template v-if="rows">
        <p v-if="rows.length === 0" class="board-empty">为主播点个赞，成为第一个上榜的人。</p>
        <div v-else class="board-scroll" tabindex="0" aria-label="本场点赞排名">
          <table>
            <thead><tr><th scope="col">排名</th><th scope="col">用户</th><th scope="col">点赞</th></tr></thead>
            <tbody>
              <tr v-for="row in rows" :key="row.user_id" :class="{ self: row.user_id === userId }">
                <td><span class="rank" :data-rank="row.rank">{{ String(row.rank).padStart(2, '0') }}</span></td>
                <th scope="row"><div class="board-user"><span class="user-avatar" aria-hidden="true">{{ initial(row.username) }}</span><span class="username" :title="row.username">{{ row.username }}</span><small v-if="row.user_id === userId" class="me-label">我</small></div></th>
                <td class="like-value">{{ row.count }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="mine !== null" class="personal"><span>{{ onBoard ? '已上榜 · 我的本场点赞' : '我的本场点赞' }}</span><strong>{{ mine }}</strong></div>
      </template>
    </div>
  </details>
</template>

<style scoped>
.like-board { flex-shrink: 0; background: #fff; border: 1px solid var(--border); border-radius: 16px; overflow: hidden; }
summary { display: flex; align-items: center; justify-content: space-between; gap: 8px; list-style: none; min-height: 58px; padding: 0 18px; cursor: pointer; }
summary::-webkit-details-marker { display: none; }
.board-title { display: flex; align-items: center; gap: 8px; min-width: 0; }
.board-title > svg { width: 20px; height: 20px; color: #dcac52; }
h2 { margin: 0; font-size: 15px; font-weight: 700; }
.top-label { margin-left: 1px; font-size: 9px; font-weight: 700; letter-spacing: .7px; color: #b3a08c; }
.collapse-icon { width: 15px; height: 15px; color: #a5a9b5; }
[open] .collapse-icon { transform: rotate(180deg); }
.board-body { border-top: 1px solid var(--border-soft); }
.board-scroll { max-height: 174px; overflow: auto; scrollbar-width: thin; scrollbar-color: #dce0e8 transparent; }
table { width: 100%; table-layout: fixed; border-collapse: collapse; font-variant-numeric: tabular-nums; }
th, td { padding: 8px 0; text-align: left; }
th:first-child, td:first-child { width: 58px; padding-left: 18px; }
th:last-child, td:last-child { width: 86px; padding-right: 18px; text-align: right; overflow-wrap: anywhere; }
thead th { position: sticky; top: 0; z-index: 1; background: #fcfcfe; padding-top: 7px; padding-bottom: 7px; color: #9da1ad; font-size: 10px; font-weight: 400; }
tbody th { font-weight: 500; min-width: 0; }
.rank { font-size: 12px; font-weight: 700; font-style: italic; color: #b5b8c3; }
.rank[data-rank="1"] { color: #df9d31; }
.rank[data-rank="2"] { color: #8999b8; }
.rank[data-rank="3"] { color: #bf967f; }
.board-user { display: flex; align-items: center; gap: 8px; min-width: 0; }
.user-avatar { display: grid; place-items: center; flex-shrink: 0; width: 25px; height: 25px; border-radius: 50%; background: #f0eefb; color: #9388c1; font-size: 10px; }
.username { color: #525869; font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.me-label { flex-shrink: 0; color: var(--accent); font-size: 10px; }
.like-value { color: #8e83c8; font-size: 12px; font-weight: 600; }
.self { background: #f8f7ff; }
.personal { display: flex; justify-content: space-between; gap: 12px; padding: 10px 18px; border-top: 1px solid var(--border-soft); color: #9599a7; font-size: 11px; }
.personal strong { color: var(--accent); font-weight: 600; }
.board-empty, .board-status { margin: 0; padding: 20px 18px; color: #969aa8; font-size: 12px; line-height: 1.8; }
</style>
