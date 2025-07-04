<template>
  <div class="p-6 max-w-3xl mx-auto text-white">
    <h1 class="text-3xl font-bold mb-6 text-center">播放量排行榜（前{{ ranks.length }}）</h1>
    <ul class="space-y-4">
      <li
        v-for="(track, index) in ranks"
        :key="track.id"
        class="p-4 bg-gradient-to-r from-purple-900 to-gray-800 rounded-xl hover:shadow-xl transition duration-300"
      >
        <div class="flex items-center space-x-4">
          <div class="text-2xl font-bold text-yellow-400 w-8 text-center">
            {{ index + 1 }}
          </div>
          <div class="flex-1">
            <div class="text-lg font-semibold">
              {{ extractTrackName(track.title) }} -- {{ extractArtistName(track.title) }}
            </div>
            <div class="text-sm text-gray-300">
              {{ decodeURIComponent(track.artist).replace(/\+/g, ' ') }} · {{ track.playcount }} 次播放
            </div>
          </div>
        </div>
      </li>
    </ul>

    <div class="mt-6 text-center" v-if="showLoadMore">
      <button
        @click="loadMore"
        class="bg-blue-600 px-6 py-2 rounded-xl hover:bg-blue-700 transition"
        :disabled="loading"
      >
        {{ loading ? '加载中...' : '查看更多' }}
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const ranks = ref([])
const offset = ref(0)
const limit = 10
const loading = ref(false)
const showLoadMore = ref(true)

onMounted(() => {
  loadRanks()
})

async function loadRanks() {
  loading.value = true
  try {
    const res = await fetch(`/api/music/rank?offset=${offset.value}&limit=${limit}`)
    const data = await res.json()
    if (offset.value === 0) {
      ranks.value = data
    } else {
      ranks.value.push(...data)
    }
    if (data.length < limit) {
      // 说明没有更多数据了
      showLoadMore.value = false
    }
  } catch (e) {
    console.error('获取排行榜失败', e)
  } finally {
    loading.value = false
  }
}

function loadMore() {
  offset.value += limit
  loadRanks()
}

// 和之前一样的辅助函数
const extractTrackName = (title) => {
  try {
    const parts = title.split('/_/')
    return decodeURIComponent(parts[1] || title).replace(/\+/g, ' ')
  } catch {
    return decodeURIComponent(title).replace(/\+/g, ' ')
  }
}

const extractArtistName = (title) => {
  try {
    const parts = title.split('/_/')
    return decodeURIComponent(parts[0] || "").replace(/\+/g, ' ')
  } catch {
    return ""
  }
}
</script>
