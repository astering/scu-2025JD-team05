<template>
  <main class="flex-1 container mx-auto px-4 py-8">
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
      <!-- 左侧两栏 -->
      <div class="lg:col-span-2 space-y-8">
        <SongList
          :songs="songs"
          @play="playSong"
          @viewAll="viewAll"
        />
        <AlbumList
          :albums="albums"
          @viewAlbum="viewAlbum"
          @viewAll="viewAll"
        />
      </div>

      <!-- 右侧单栏 -->
      <div class="space-y-8">
        <!-- 美化后的 Top10 歌手卡片 -->
        <section class="bg-white/5 border border-white/10 backdrop-blur-md rounded-2xl p-6 shadow-md">
          <div class="flex items-center mb-4">
            <h2 class="text-xl font-bold text-white flex items-center">
              <i class="fas fa-crown text-yellow-400 mr-2"></i>
              Top10 歌手
            </h2>
            <button
              @click="() => viewAll('artists')"
              class="ml-auto text-sm text-blue-400 hover:text-blue-300"
            >
              查看全部 <i class="fas fa-chevron-right ml-1"></i>
            </button>
          </div>

          <ul class="divide-y divide-white/10">
            <li
              v-for="(artist, index) in topArtists"
              :key="artist.id"
              class="py-3 px-2 cursor-pointer hover:bg-white/5 rounded-lg transition"
              @click="viewArtist(artist)"
            >
              <div class="flex justify-between items-center">
                <div>
                  <div class="font-semibold text-white">
                    {{ index + 1 }}. {{ artist.name.replace(/\+/g, ' ') }}
                  </div>
                  <div class="text-sm text-gray-400">
                    播放量: {{ artist.playcount.toLocaleString() }}
                  </div>
                </div>
                <i class="fas fa-microphone-alt text-purple-400"></i>
              </div>
            </li>
          </ul>
        </section>
        <ArtistList
          :artists="artists"
          @viewArtist="viewArtist"
          @viewAll="viewAll"
        />
      </div>
    </div>
  </main>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from "vue";
import axios from "axios";
import { useRouter } from "vue-router";

const router = useRouter();
import SongList from "./SongList.vue";

// 推荐歌曲
const songs = ref([]);

// 后端加载的 topArtists 和 artists（统一来源）
const topArtists = ref([]);
const artists = ref([]);

async function fetchRecommendSongs() {
  const user = JSON.parse(localStorage.getItem("user"));
  if (user?.user_id) {
    try {
      const res = await axios.get("http://127.0.0.1:8000/api/music/recommend", {
        params: { user_id: user.user_id }
      });
      songs.value = res.data.tracks.map(track => {
        const replaced = track.title.replace(/\+/g, ' ');
        const [artistName, songName] = replaced.split('/_/');
        return {
          id: track.id,
          title: songName ?? replaced,
          artist: artistName ?? "推荐系统",
          duration: `推荐评分: ${track.score ?? track.pred_score ?? 'N/A'}`
        };
      });
    } catch (error) {
      console.error("获取推荐歌曲失败", error);
    }
  } else {
    console.warn("用户未登录或 user_id 缺失，无法获取推荐");
  }
}

// 获取后端返回的 top_artists 数据
async function fetchArtists() {
  try {
    const res = await axios.get("http://127.0.0.1:8000/api/artist/top-artists");
    const list = res.data.map(item => ({
      id: item.id,
      name: item.name,
      playcount: item.playcount,
    }));
    topArtists.value = list.slice(0, 10); // Top 10 展示
    artists.value = list; // 全部 artist 数据
  } catch (error) {
    console.error("获取歌手信息失败", error);
  }
}

onMounted(() => {
  fetchRecommendSongs();
  fetchArtists();
  window.addEventListener("user-logged-in", fetchRecommendSongs);
});

onBeforeUnmount(() => {
  window.removeEventListener("user-logged-in", fetchRecommendSongs);
});

const albums = ref([
  { id: 1, title: "浪漫之歌", artist: "巴赫", year: "2010" },
  { id: 2, title: "Good Vibes", artist: "Adele", year: "2017" },
  { id: 3, title: "Kaleidoscope", artist: "Coldplay", year: "2019" },
]);


function viewAll(type) {
  if (type === 'artists') {
    router.push("/artists");
  } else {
    alert(`查看全部：${type}`);
  }
}
</script>
