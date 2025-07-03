<template>
  <section class="card-glass rounded-xl p-6">
    <div class="flex items-center mb-6">
      <h2 class="text-xl font-bold flex items-center">
        <i class="fas fa-music text-purple-400 mr-2"></i>
        推荐歌曲
      </h2>
      <button
        @click="() => viewAll('songs')"
        class="ml-auto text-purple-400 hover:text-purple-300 text-sm"
      >
        查看全部 <i class="fas fa-chevron-right ml-1"></i>
      </button>
    </div>

    <div class="space-y-4">
      <div
        v-for="song in songs"
        :key="song.id"
        class="flex items-center p-4 hover:bg-slate-800/50 rounded-lg transition cursor-pointer card-hover"
      >
        <div
          class="w-12 h-12 rounded-lg bg-gradient-to-br from-purple-600 to-pink-500 flex items-center justify-center mr-4"
          @click="playSong(song)"
        >
          <i class="fas fa-play text-white"></i>
        </div>
        <div class="flex-1" @click="playSong(song)">
          <h3 class="font-semibold">{{ song.title }}</h3>
          <p class="text-gray-400 text-sm">{{ song.artist }}</p>
        </div>
        <div class="flex items-center space-x-4">
          <button
            class="text-gray-500 hover:text-white"
            @click.prevent="likeSong(song)"
            title="喜欢"
          >
            <i class="far fa-heart"></i>
          </button>
          <button
            class="text-gray-500 hover:text-white"
            @click.prevent="dislikeSong(song)"
            title="不喜欢"
          >
            <i class="fas fa-thumbs-down"></i>
          </button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup>
import { defineProps, defineEmits } from "vue";

const props = defineProps({
  songs: {
    type: Array,
    required: true,
  },
});
const emit = defineEmits(["play", "viewAll"]);

function playSong(song) {
  emit("play", song);
  alert(`播放歌曲：${song.title} - ${song.artist}`);
}

function viewAll(type) {
  emit("viewAll", type);
  alert(`查看全部：${type}`);
}

function likeSong(song) {
  alert(`喜欢歌曲：’${song.title}‘`);
}
function dislikeSong(song) {
  alert(`不喜欢歌曲：’${song.title}‘`);
}
</script>
