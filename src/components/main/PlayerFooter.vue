<!-- 文件: /src/components/PlayerFooter.vue -->
<template>
  <footer class="sticky bottom-0 z-10 bg-slate-900/90 backdrop-blur-lg border-t border-slate-800">
    <div class="container mx-auto px-4 py-3">
      <div class="flex items-center">
        <!-- 当前播放 -->
        <div class="flex items-center w-1/4">
          <div class="w-14 h-14 rounded-lg bg-gradient-to-br from-purple-600 to-pink-500 mr-4 flex items-center justify-center">
            <i class="fas fa-play text-white"></i>
          </div>
          <div>
            <h3 class="font-semibold">贝塔特</h3>
            <p class="text-gray-400 text-sm">爱奇艺·普莱德曼</p>
          </div>
          <button class="ml-4 text-gray-400 hover:text-white player-btn">
            <i class="far fa-heart"></i>
          </button>
        </div>

        <!-- 播放控制 -->
        <div class="flex-1 flex flex-col items-center">
          <div class="flex items-center space-x-6">
            <button class="text-gray-400 hover:text-white player-btn">
              <i class="fas fa-random"></i>
            </button>
            <button class="text-gray-400 hover:text-white player-btn">
              <i class="fas fa-step-backward"></i>
            </button>
            <button class="w-10 h-10 rounded-full bg-purple-600 flex items-center justify-center text-white hover:bg-purple-500 transition player-btn">
              <i class="fas fa-play"></i>
            </button>
            <button class="text-gray-400 hover:text-white player-btn">
              <i class="fas fa-step-forward"></i>
            </button>
            <button class="text-gray-400 hover:text-white player-btn">
              <i class="fas fa-repeat"></i>
            </button>
          </div>

          <div class="w-full max-w-2xl mt-2 flex items-center space-x-3">
            <span class="text-gray-400 text-xs">1:23</span>
            <div class="flex-1 progress-bar">
              <div class="progress-fill"></div>
            </div>
            <span class="text-gray-400 text-xs">3:45</span>
          </div>
        </div>

        <!-- 音量控制 -->
        <div class="flex items-center justify-end w-1/4 space-x-3">
          <i class="fas fa-list text-gray-400"></i>
          <i class="fas fa-volume-up text-gray-400"></i>
          <div class="w-24 progress-bar">
            <div class="progress-fill"></div>
          </div>
        </div>
      </div>
    </div>
  </footer>
</template>

<script setup>
</script>

<style scoped>
.progress-bar {
  height: 4px;
  background: rgba(255, 255, 255, 0.2);
  border-radius: 2px;
  overflow: hidden;
}
.progress-fill {
  height: 100%;
  background: linear-gradient(90deg, #8b5cf6, #ec4899);
  width: 35%;
}
.player-btn {
  transition: all 0.2s;
}
.player-btn:hover {
  color: white;
  transform: scale(1.1);
}
</style>
