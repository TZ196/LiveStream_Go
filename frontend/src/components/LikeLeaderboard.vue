<script setup lang="ts">
import type { BoardRow } from '../leaderboard'

defineProps<{ rows: BoardRow[] | null; mine: string | null; onBoard: boolean; status: string; userId: string }>()
</script>

<template>
  <section class="like-board" aria-labelledby="board-title">
    <h2 id="board-title">本场点赞榜</h2>
    <p v-if="status" role="status">{{ status }}</p>
    <template v-if="rows">
      <p v-if="rows.length === 0">本场还没有人点赞。</p>
      <div v-else class="board-scroll" tabindex="0" aria-label="本场点赞排名">
        <table>
          <thead><tr><th scope="col">排名</th><th scope="col">用户</th><th scope="col">点赞数</th></tr></thead>
          <tbody>
            <tr v-for="row in rows" :key="row.user_id" :class="{ self: row.user_id === userId }">
              <td>{{ row.rank }}</td>
              <th scope="row">{{ row.username }}<span v-if="row.user_id === userId">（我）</span></th>
              <td>{{ row.count }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-if="!onBoard && mine !== null" class="personal">我的点赞：{{ mine }}</p>
    </template>
  </section>
</template>

<style scoped>
.like-board { margin-block: 1.5rem; border-top: 1px solid #dce5e4; }
.like-board h2 { font-size: 1.15rem; margin-block: 1.1rem .75rem; }
.board-scroll { max-height: 22rem; overflow: auto; }
table { width: 100%; border-collapse: separate; border-spacing: 0; font-variant-numeric: tabular-nums; }
th, td { padding: .5rem .65rem; text-align: left; border-bottom: 1px solid #e5ecea; overflow-wrap: anywhere; }
thead th { position: sticky; top: 0; background: #f4f8f7; }
th:last-child, td:last-child { text-align: right; }
tbody th { font-weight: 500; }
.self { background: #e6f5ee; color: #065f46; }
.personal { font-weight: 600; }
</style>
