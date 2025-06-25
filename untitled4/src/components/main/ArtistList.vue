<template>
  <section class="card-glass rounded-xl p-6">
    <div class="flex items-center mb-6">
      <h2 class="text-xl font-bold flex items-center">
        <i class="fas fa-microphone text-red-400 mr-2"></i>
        推荐歌手
      </h2>
      <button
        @click="() => viewAll('artists')"
        class="ml-auto text-purple-400 hover:text-purple-300 text-sm"
      >
        查看全部 <i class="fas fa-chevron-right ml-1"></i>
      </button>
    </div>

    <div class="grid grid-cols-2 gap-4">
      <div
        v-for="artist in artists"
        :key="artist.id"
        @click="() => viewArtist(artist)"
        class="flex flex-col items-center p-4 bg-slate-800/50 rounded-xl hover:bg-slate-800 transition cursor-pointer card-hover"
      >
        <div
          class="w-16 h-16 rounded-full bg-gradient-to-br from-yellow-600 to-red-500 mb-3 flex items-center justify-center"
        >
          <i class="fas fa-microphone-alt text-white text-sm"></i>
        </div>
        <h3 class="font-semibold text-center">{{ artist.name }}</h3>
        <p class="text-gray-400 text-sm text-center">{{ artist.info }}</p>
        <p class="text-gray-500 text-sm mt-1">{{ artist.followers }} 粉丝</p>
      </div>
    </div>
  </section>
</template>

<script setup>
import { defineProps, defineEmits } from "vue";

const props = defineProps({
  artists: {
    type: Array,
    required: true,
  },
});
const emit = defineEmits(["viewArtist", "viewAll"]);

function viewArtist(artist) {
  emit("viewArtist", artist);
  alert(`查看歌手：${artist.name}`);
}
function viewAll(type) {
  emit("viewAll", type);
  alert(`查看全部：${type}`);
}
</script>
