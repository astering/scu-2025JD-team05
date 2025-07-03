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
        <TopArtists
          :topArtists="topArtists"
          @viewAll="viewAll"
        />
        <PlaylistList
          :playlists="playlists"
          @viewPlaylist="viewPlaylist"
          @viewAll="viewAll"
        />
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

import SongList from "./SongList.vue";
import AlbumList from "./AlbumList.vue";
import PlaylistList from "./PlayListList.vue";
import ArtistList from "./ArtistList.vue";
import TopArtists from "./TopArtists.vue";

const songs = ref([]);

async function fetchRecommendSongs() {
  const user = JSON.parse(localStorage.getItem("user"));
  if (user?.user_id) {
    try {
      const res = await axios.get("http://127.0.0.1:8000/api/music/recommend", {
        params: { user_id: user.user_id }
      });
      songs.value = res.data.tracks.map(track => {
        // 原始track.title格式: "Bedouin+Soundclash/_/GHM+REEDIT"
        const replaced = track.title.replace(/\+/g, ' ');
        const [artistName, songName] = replaced.split('/_/');
        return {
          id: track.id,
          title: songName ?? replaced,       // 歌曲名，找不到则用原字符串
          artist: artistName ?? "推荐系统",  // 歌手名，找不到则默认“推荐系统”
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


onMounted(() => {
  fetchRecommendSongs();

  // 监听登录成功事件，自动刷新推荐列表
  window.addEventListener('user-logged-in', fetchRecommendSongs);
});

onBeforeUnmount(() => {
  window.removeEventListener('user-logged-in', fetchRecommendSongs);
});

const albums = ref([
  { id: 1, title: "浪漫之歌", artist: "巴赫", year: "2010" },
  { id: 2, title: "Good Vibes", artist: "Adele", year: "2017" },
  { id: 3, title: "Kaleidoscope", artist: "Coldplay", year: "2019" },
]);

const playlists = ref([
  { id: 1, title: "心情电台", creator: "管理员", tracks: 24 },
  { id: 2, title: "摇滚精选", creator: "用户A", tracks: 18 },
  { id: 3, title: "夜晚节奏", creator: "DJ B", tracks: 30 },
]);

const artists = ref([
  { id: 1, name: "Taylor Swift", info: "流行", followers: 9800000 },
  { id: 2, name: "The Weekend", info: "R&B", followers: 9200000 },
  { id: 3, name: "Ed Sheeran", info: "流行", followers: 8900000 },
  { id: 4, name: "BTS", info: "K-Pop", followers: 8700000 },
  { id: 5, name: "Drake", info: "嘻哈", followers: 8500000 },
]);

const topArtists = ref([
  { id: 1, name: "Taylor Swift", info: "流行", popularity: 9800 },
  { id: 2, name: "The Weekend", info: "R&B", popularity: 9200 },
  { id: 3, name: "Ed Sheeran", info: "流行", popularity: 8900 },
  { id: 4, name: "BTS", info: "K-Pop", popularity: 8700 },
  { id: 5, name: "Drake", info: "嘻哈", popularity: 8500 },
]);

function playSong(song) {
  alert(`播放歌曲：${song.title} - ${song.artist}`);
}
function viewAlbum(album) {
  alert(`查看专辑：${album.title} - ${album.artist}`);
}
function viewPlaylist(playlist) {
  alert(`查看歌单：${playlist.title} by ${playlist.creator}`);
}
function viewArtist(artist) {
  alert(`查看歌手：${artist.name}`);
}
function viewAll(type) {
  alert(`查看全部：${type}`);
}
</script>
