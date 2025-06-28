<template>
  <section class="card-glass rounded-xl p-6">
    <div class="flex items-center mb-6">
      <h2 class="text-xl font-bold flex items-center">
        <i class="fas fa-compact-disc text-pink-400 mr-2"></i>
        推荐专辑
      </h2>
      <button
        @click="() => viewAll('albums')"
        class="ml-auto text-purple-400 hover:text-purple-300 text-sm"
      >
        查看全部 <i class="fas fa-chevron-right ml-1"></i>
      </button>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
      <div
        v-for="album in albums"
        :key="album.id"
        @click="() => viewAlbum(album)"
        class="bg-slate-800/50 rounded-xl p-4 hover:bg-slate-800 transition cursor-pointer card-hover"
      >
        <div
          class="w-full aspect-square rounded-xl bg-gradient-to-br from-purple-600 to-pink-500 mb-4 flex items-center justify-center"
        >
          <i class="fas fa-compact-disc text-white text-3xl opacity-70"></i>
        </div>
        <h3 class="font-semibold truncate">{{ album.title }}</h3>
        <p class="text-gray-400 text-sm">{{ album.artist }}</p>
        <p class="text-gray-500 text-sm mt-1">{{ album.year }}</p>
      </div>
    </div>
  </section>
</template>

<script setup>
import { defineProps, defineEmits } from "vue";

const props = defineProps({
  albums: {
    type: Array,
    required: true,
  },
});
const emit = defineEmits(["viewAlbum", "viewAll"]);

function viewAlbum(album) {
  emit("viewAlbum", album);
  alert(`查看专辑：${album.title} - ${album.artist}`);
}

function viewAll(type) {
  emit("viewAll", type);
  alert(`查看全部：${type}`);
}
</script>
