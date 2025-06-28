<template>
  <header class="sticky top-0 z-20 bg-slate-900/90 backdrop-blur-md border-b border-slate-800">
    <div class="container mx-auto px-4 py-3 flex items-center justify-between">
      <div class="flex items-center space-x-3">
        <div class="w-10 h-10 rounded-full gradient-bg flex items-center justify-center">
          <i class="fas fa-music text-white"></i>
        </div>
        <h1 class="text-xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent">
          音乐推荐系统
        </h1>
      </div>

      <slot name="search"></slot>

      <div class="flex items-center space-x-4">
        <button @click="notify" class="p-2 rounded-full hover:bg-slate-800 transition">
          <i class="fas fa-bell text-gray-300"></i>
        </button>
        <button @click="settings" class="p-2 rounded-full hover:bg-slate-800 transition">
          <i class="fas fa-cog text-gray-300"></i>
        </button>
        <button @click="userProfile" class="flex items-center space-x-2 bg-slate-800 px-3 py-1 rounded-full hover:bg-slate-700 transition">
          <i class="fas fa-user text-gray-300"></i>
          <span>用户</span>
        </button>
      </div>
    </div>

    <nav class="border-b border-slate-800">
      <div class="container mx-auto px-4 flex space-x-6">
        <button
          v-for="(menu, index) in menus"
          :key="index"
          @click="selectMenu(index)"
          :class="[
            'py-3 px-1 font-medium transition cursor-pointer',
            currentMenu === index
              ? 'border-b-2 border-purple-500 text-white'
              : 'text-gray-400 hover:text-white',
          ]"
        >
          {{ menu.label }}
        </button>
      </div>
    </nav>
  </header>
</template>

<script setup>
import { ref } from "vue";
import { useRouter } from "vue-router";
const emit = defineEmits(["menuChanged"]);
const router = useRouter();

// 每个菜单项包含 label 和 path
const menus = [
  { label: "发现", path: "/home" },
  { label: "排行榜", path: "/rank" },
  { label: "歌单", path: "/playlist" },
  { label: "歌手", path: "/artists" },
  { label: "最新音乐", path: "/latest" },
  { label: "驾驶舱", path: "/dashboard" }, //
];

const currentMenu = ref(0);

function selectMenu(index) {
  currentMenu.value = index;
  emit("menuChanged", index);
  router.push(menus[index].path); //
}

function notify() {
  alert("你点击了通知按钮");
}
function settings() {
  alert("你点击了设置按钮");
}
function userProfile() {
  alert("你点击了用户按钮");
}
</script>
