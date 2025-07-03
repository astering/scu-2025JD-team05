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

      <div class="flex items-center space-x-4 relative">
        <button @click="notify" class="p-2 rounded-full hover:bg-slate-800 transition">
          <i class="fas fa-bell text-gray-300"></i>
        </button>
        <button @click="settings" class="p-2 rounded-full hover:bg-slate-800 transition">
          <i class="fas fa-cog text-gray-300"></i>
        </button>
        <div class="relative">
          <button @click="toggleDropdown" class="flex items-center space-x-2 bg-slate-800 px-3 py-1 rounded-full hover:bg-slate-700 transition">
            <i class="fas fa-user text-gray-300"></i>
            <span>用户</span>
          </button>

          <!-- 下拉菜单 -->
          <div v-if="showDropdown" class="absolute right-0 mt-2 w-28 bg-white rounded shadow-md z-30">
            <button @click="logout" class="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">
              退出
            </button>
          </div>
        </div>
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
import { ref, onMounted, onBeforeUnmount } from "vue";
import { useRouter } from "vue-router";
const emit = defineEmits(["menuChanged"]);
const router = useRouter();

const menus = [
  { label: "发现", path: "/home" },
  { label: "排行榜", path: "/rank" },
  { label: "你的歌单", path: "/playlist" },
  { label: "推荐歌手", path: "/artists" },
  { label: "驾驶舱", path: "/dashboard" },
];

const currentMenu = ref(0);
const showDropdown = ref(false);

function selectMenu(index) {
  currentMenu.value = index;
  emit("menuChanged", index);
  router.push(menus[index].path);
}

function notify() {
  alert("你点击了通知按钮");
}

function settings() {
  alert("你点击了设置按钮");
}

function toggleDropdown() {
  showDropdown.value = !showDropdown.value;
}

function logout() {
  // 清除本地存储的登录信息（token、用户信息等）
  localStorage.removeItem("token");
  localStorage.removeItem("userInfo");


  localStorage.clear();

  // 跳转到 login 页面
  showDropdown.value = false;
  router.push("/login");
}

// 点击页面其他区域关闭下拉菜单
function handleClickOutside(event) {
  if (!event.target.closest(".relative")) {
    showDropdown.value = false;
  }
}
onMounted(() => {
  document.addEventListener("click", handleClickOutside);
});
onBeforeUnmount(() => {
  document.removeEventListener("click", handleClickOutside);
});
</script>
