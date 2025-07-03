<template>
  <div class="flex-1 max-w-xl mx-6">
    <div class="relative">
      <input
        type="text"
        placeholder="搜索歌曲、专辑或艺术家..."
        v-model="searchQuery"
        @keyup.enter="onSearch"
        class="w-full py-2 px-4 pl-10 rounded-full bg-slate-800 text-white focus:outline-none focus:ring-2 focus:ring-purple-500"
      />
      <i class="fas fa-search absolute left-3 top-3 text-gray-400"></i>
    </div>
  </div>
</template>

<script setup>
import { ref, defineEmits } from "vue";

const emit = defineEmits(["search"]);

const searchQuery = ref("");

function onSearch() {
  if (!searchQuery.value.trim()) {
    alert("请输入搜索关键词！");
    return;
  }
  emit("search", searchQuery.value);
  alert(`搜索关键词：${searchQuery.value}`);
}
</script>