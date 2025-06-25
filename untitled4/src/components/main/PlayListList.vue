<template>
  <section class="card-glass rounded-xl p-6">
    <div class="flex items-center mb-6">
      <h2 class="text-xl font-bold flex items-center">
        <i class="fas fa-list text-green-400 mr-2"></i>
        推荐歌单
      </h2>
      <button
        @click="() => viewAll('playlists')"
        class="ml-auto text-purple-400 hover:text-purple-300 text-sm"
      >
        查看全部 <i class="fas fa-chevron-right ml-1"></i>
      </button>
    </div>

    <div class="space-y-4">
      <div
        v-for="playlist in playlists"
        :key="playlist.id"
        @click="() => viewPlaylist(playlist)"
        class="flex items-center p-3 hover:bg-slate-800/50 rounded-lg transition cursor-pointer card-hover"
      >
        <div
          class="w-16 h-16 rounded-lg bg-gradient-to-br from-blue-600 to-teal-500 mr-4 flex items-center justify-center"
        >
          <i class="fas fa-list text-white opacity-70"></i>
        </div>
        <div>
          <h3 class="font-semibold">{{ playlist.title }}</h3>
          <p class="text-gray-400 text-sm">by {{ playlist.creator }}</p>
          <p class="text-gray-500 text-sm mt-1">{{ playlist.tracks }} 首歌曲</p>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup>
import { defineProps, defineEmits } from "vue";

const props = defineProps({
  playlists: {
    type: Array,
    required: true,
  },
});
const emit = defineEmits(["viewPlaylist", "viewAll"]);

function viewPlaylist(playlist) {
  emit("viewPlaylist", playlist);
  alert(`查看歌单：${playlist.title} by ${playlist.creator}`);
}

function viewAll(type) {
  emit("viewAll", type);
  alert(`查看全部：${type}`);
}
</script>
