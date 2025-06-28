<template>
<div class="min-h-screen flex flex-col bg-gradient-to-br from-slate-900 to-slate-800 text-white">
    <Header @menuChanged="onMenuChange">
      <template #search>
        <SearchBar @search="onSearch" />
      </template>
    </Header>
    <MainContent />
<Live2DWidget />
  </div>
</template>

<script setup>
import Header from '@/components/main/Header.vue'
import SearchBar from '@/components/main/SearchBar.vue'
import MainContent from '@/components/main/MainContent.vue'
import Live2DWidget from "@/components/live2d/Live2DWidget.vue";

function onMenuChange(index) {
  console.log("菜单切换到索引:", index)
}
function onSearch(query) {
  console.log("搜索关键词:", query)
}
</script>
