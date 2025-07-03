# MIR 音乐大数据分析

## 使用数据集

- MSD（Million Song Dataset）
- MSD's Lastfm
- 30Music

## 可视化

### MSD

#### 歌曲按年份分布

![songs-number-per-year](https://s2.loli.net/2025/07/03/uRd5j27HqGhD9sA.jpg)

#### 艺术家地理分布

![artist_distribution](https://s2.loli.net/2025/07/03/KOqDH2vp47nQ5Ij.png)

#### 艺术家热度分布

![artist-2025-07-03T03-26-30.356Z](https://s2.loli.net/2025/07/03/TjEJc59FHR1vNYq.jpg)

#### 流派分布图

##### 细分版

![genre细分stack](https://s2.loli.net/2025/07/03/YdEDBpcQm3FkAVW.jpg)

![genre细分expand](https://s2.loli.net/2025/07/03/crSYUBHsQzh6wkF.jpg)

##### 大类

![genre-year-stack-2025-07-03T03-26-39.503Z](https://s2.loli.net/2025/07/03/aWZ5KFknGsP1SxE.jpg)

![genre-year-expand-2025-07-03T03-26-49.430Z](https://s2.loli.net/2025/07/03/OgRTcZnpStzxwHP.jpg)

#### 声学特征分析

##### 总表

![acoustic-2025-07-03T03-24-03.342Z](https://s2.loli.net/2025/07/03/oc2urLOzEjX3QHC.jpg)

##### 歌曲时长

![acoustic-duration-2025-07-03T03-25-36.285Z](https://s2.loli.net/2025/07/03/QZ3XocS41kNhzOt.jpg)

##### 歌曲前奏

![acoustic-fade-in-2025-07-03T03-25-26.736Z](https://s2.loli.net/2025/07/03/heq2B3JHsFtPjpw.jpg)

##### 歌曲音高

![acoustic-key-2025-07-03T03-25-17.615Z](https://s2.loli.net/2025/07/03/dVMJblt3UK5hLcs.jpg)

##### 歌曲响度

![acoustic-loudness-2025-07-03T03-24-46.150Z](https://s2.loli.net/2025/07/03/TvdeKaAuBZzocX9.jpg)

##### 歌曲速度

![acoustic-tempo-2025-07-03T03-25-43.797Z](https://s2.loli.net/2025/07/03/9fbi3Y4BxrSOMdJ.jpg)

### 30Music

#### 全球用户数量热力图

![全球用户数量热力图](https://s2.loli.net/2025/07/03/nh4tQoScdLETa8H.jpg)

#### 听歌活跃时间分布

##### 按小时

![听歌活跃时间分布（按小时）](https://s2.loli.net/2025/07/03/u84R3vdwForsjWU.png)

##### 按星期

![听歌活跃时间分布（按星期）](https://s2.loli.net/2025/07/03/s5oHIPqtDTClknp.png)

##### 按月份

![听歌活跃时间分布（按月份）](https://s2.loli.net/2025/07/03/kVDHarIUGSEXeMP.png)

#### 流行音乐风格

![每个出生年代播放量前10的音乐风格（堆叠柱状图）](https://s2.loli.net/2025/07/03/lV5T4vZeXKoMt8j.png)

![每个出生年代播放量前10的音乐风格（热力图）](https://s2.loli.net/2025/07/03/cYxSfmq6yR8isXN.png)

## 结论

### 数据集

1. MSD数据集侧重歌曲自身特征，独立于听众
2. Last.fm带有歌曲的外部特征，如热度、相似度等
3. 30Music具有用户信息，可以用于用户行为分析

### 分布与变化

1. 随着年代变化，歌曲的响度总体增大，行进速度略有增加，时长也有延长的趋势，而音高变化不大
2. 艺术家统计数据在全球的分布不均，主要集中在北美和西欧发达国家，歌曲自然以英语系歌曲为主
3. 早期流行的音乐流派如蓝调blue等，在近些年已逐渐销声匿迹，摇滚rock和流行pop后来居上
