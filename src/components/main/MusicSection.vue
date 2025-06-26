<!-- 文件: /src/components/MusicSection.vue -->
<template>
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
    <!-- 左侧内容 -->
    <div class="lg:col-span-2 space-y-8">
      <!-- 推荐歌曲 -->
      <section class="card-glass rounded-xl p-6">
        <div class="flex items-center mb-6">
          <h2 class="text-xl font-bold flex items-center">
            <i class="fas fa-music text-purple-400 mr-2"></i>
            推荐歌曲
          </h2>
        </div>
        <div class="space-y-4">
          <div v-for="song in songs" :key="song.id" class="flex items-center p-4 hover:bg-slate-800/50 rounded-lg transition cursor-pointer card-hover">
            <div class="w-12 h-12 rounded-lg bg-gradient-to-br from-purple-600 to-pink-500 flex items-center justify-center mr-4">
              <i class="fas fa-play text-white"></i>
            </div>
            <div class="flex-1">
              <h3 class="font-semibold">{{ song.title }}</h3>
              <p class="text-gray-400 text-sm">{{ song.artist }}</p>
            </div>
            <div class="flex items-center space-x-6">
              <span class="text-gray-500 text-sm">{{ song.year }}</span>
              <span class="text-gray-500 text-sm">{{ song.duration }}</span>
            </div>
          </div>
        </div>
      </section>

      <!-- 推荐专辑 -->
      <section class="card-glass rounded-xl p-6">
        <div class="flex items-center mb-6">
          <h2 class="text-xl font-bold flex items-center">
            <i class="fas fa-compact-disc text-pink-400 mr-2"></i>
            推荐专辑
          </h2>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div v-for="album in albums" :key="album.id" class="bg-slate-800/50 rounded-xl p-4 hover:bg-slate-800 transition cursor-pointer card-hover">
            <div class="w-full aspect-square rounded-xl bg-gradient-to-br from-purple-600 to-pink-500 mb-4 flex items-center justify-center">
              <i class="fas fa-compact-disc text-white text-3xl opacity-70"></i>
            </div>
            <h3 class="font-semibold truncate">{{ album.title }}</h3>
            <p class="text-gray-400 text-sm">{{ album.artist }}</p>
            <p class="text-gray-500 text-sm mt-1">{{ album.year }}</p>
          </div>
        </div>
      </section>
    </div>

    <!-- 右侧内容 -->
    <div class="space-y-8">
      <!-- 推荐歌手 -->
      <section class="card-glass rounded-xl p-6">
        <div class="flex items-center mb-6">
          <h2 class="text-xl font-bold flex items-center">
            <i class="fas fa-microphone text-red-400 mr-2"></i>
            推荐歌手
          </h2>
        </div>
        <div class="grid grid-cols-2 gap-4">
          <div v-for="artist in artists" :key="artist.id" class="flex flex-col items-center p-4 bg-slate-800/50 rounded-xl hover:bg-slate-800 transition cursor-pointer card-hover">
            <div class="w-16 h-16 rounded-full bg-gradient-to-br from-yellow-600 to-red-500 mb-3 flex items-center justify-center">
              <i class="fas fa-microphone-alt text-white text-sm"></i>
            </div>
            <h3 class="font-semibold text-center">{{ artist.name }}</h3>
            <p class="text-gray-400 text-sm text-center">{{ artist.info }}</p>
            <p class="text-gray-500 text-sm mt-1">{{ artist.followers }} 粉丝</p>
          </div>
        </div>
      </section>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const songs = ref([
  { id: 1, title: '贝塔特', artist: '爱奇艺·普莱德曼', year: '1928', duration: '3:45' },
  { id: 2, title: 'Share of You', artist: '马赫威尔', year: '1939', duration: '4:22' },
  { id: 3, title: 'Briefing Life for Time Festival', artist: 'Various Artists', year: '2023', duration: '3:30' }
])

const albums = ref([
  { id: 1, title: 'Ed Robert II', artist: 'Drobin', year: '2017' },
  { id: 2, title: 'After Piano', artist: 'The Weekend', year: '2020' },
  { id: 3, title: 'Recovery', artist: 'Emotion', year: '2019' }
])

const artists = ref([
  { id: 1, name: 'Taylor Swift', info: '邓迪 (Aug)', followers: '2.4M' },
  { id: 2, name: '音乐会', info: '风格 - 生活行', followers: '1.1M' },
  { id: 3, name: 'Bills Film', info: '风格 Interactive', followers: '850K' }
])
</script>

