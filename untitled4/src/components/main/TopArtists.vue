<template>
  <section class="card-glass rounded-xl p-6">
    <div class="flex items-center mb-6">
      <h2 class="text-xl font-bold flex items-center">
        <i class="fas fa-chart-line text-yellow-400 mr-2"></i>
        2024年Top歌手
      </h2>
      <button
        @click="() => viewAll('topArtists')"
        class="ml-auto text-purple-400 hover:text-purple-300 text-sm"
      >
        完整榜单 <i class="fas fa-chevron-right ml-1"></i>
      </button>
    </div>

    <table class="w-full">
      <thead>
        <tr
          class="text-left text-gray-400 text-sm border-b border-slate-800"
        >
          <th class="pb-3">排名</th>
          <th class="pb-3">歌手</th>
          <th class="pb-3 text-right">热度</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="(artist, index) in topArtists"
          :key="index"
          class="border-b border-slate-800 hover:bg-slate-800/50 transition"
        >
          <td class="py-3 font-bold text-lg">{{ index + 1 }}</td>
          <td class="py-3">
            <div class="flex items-center">
              <div
                class="w-10 h-10 rounded-full bg-gradient-to-br from-yellow-600 to-red-500 mr-3 flex items-center justify-center"
              >
                <i class="fas fa-microphone text-white text-sm"></i>
              </div>
              <div>
                <div class="font-medium">{{ artist.name }}</div>
                <div class="text-gray-400 text-sm">{{ artist.info }}</div>
              </div>
            </div>
          </td>
          <td class="py-3 text-right font-mono">{{ artist.popularity }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup>
import { defineProps, defineEmits } from "vue";

const props = defineProps({
  topArtists: {
    type: Array,
    required: true,
  },
});
const emit = defineEmits(["viewAll"]);

function viewAll(type) {
  emit("viewAll", type);
  alert(`查看全部：${type}`);
}
</script>
