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
import { ref } from "vue";

import SongList from "./SongList.vue";
import AlbumList from "./AlbumList.vue";
import PlaylistList from "./PlayListList.vue";
import ArtistList from "./ArtistList.vue";
import TopArtists from "./TopArtists.vue";

const songs = ref([
  { id: 1, title: "贝塔特", artist: "爱奇艺·普莱德曼", year: "1928", duration: "3:45" },
  { id: 2, title: "Share of You", artist: "马赫威尔", year: "1939", duration: "4:22" },
  { id: 3, title: "Purple Rain", artist: "The Weekend", year: "1984", duration: "5:02" },
  { id: 4, title: "Rain on Me", artist: "Lady Gaga", year: "2010", duration: "3:55" },
  { id: 5, title: "Hallelujah", artist: "杰夫·巴克利", year: "1995", duration: "6:30" },
]);

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
