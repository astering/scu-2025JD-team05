# MIR 音乐大数据分析

## 使用数据集

- MSD（Million Song Dataset）
- MSD's Last.fm
- 30Music（Thirty Music）
- MSD's musixmatch（提供歌词分析）

## 可视化

### MSD

> 由superset接入mysql生成

#### 歌曲按年份分布

![songs-number-per-year](https://s2.loli.net/2025/07/03/uRd5j27HqGhD9sA.jpg)

#### 艺术家地理分布

![artist_distribution](https://s2.loli.net/2025/07/03/KOqDH2vp47nQ5Ij.png)

[交互版本](https://public.tableau.com/app/profile/lixie.hou/viz/test_17513319001510/1)（由tableau提供支持）

<div class='tableauPlaceholder' id='viz1751517082186' style='position: relative'><noscript><a href='#'><img alt='工作表 1 ' src='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;te&#47;test_17513319001510&#47;1&#47;1_rss.png' style='border: none' /></a></noscript><object class='tableauViz' style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /><param name='embed_code_version' value='3' /><param name='site_root' value='' /><param name='name' value='test_17513319001510&#47;1' /><param name='tabs' value='no' /><param name='toolbar' value='yes' /><param name='static_image' value='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;te&#47;test_17513319001510&#47;1&#47;1.png' /><param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='zh-CN' /></object></div><script type='text/javascript'> var divElement = document.getElementById('viz1751517082186'); var    vizElement = divElement.getElementsByTagName('object')[0]; vizElement.style.width='100%';vizElement.style.height=(divElement.offsetWidth*0.75)+'px'; var    scriptElement = document.createElement('script'); scriptElement.src =    'https://public.tableau.com/javascripts/api/viz_v1.js';    vizElement.parentNode.insertBefore(scriptElement, vizElement); </script>

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

#### musixmatch词云

![word-cloud-2025-07-04T04-15-44.300Z](https://s2.loli.net/2025/07/04/wThvQyBIPlSnYMX.jpg)

### 30Music

> 由[plotly.py](https://github.com/plotly/plotly.py)生成

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

1. MSD数据集侧重歌曲自身特征，记录了歌曲的声学信息和元数据，大部分信息独立于听众，可以进行基于内容的过滤
2. Last.fm主要提供了对MSD的补充信息，如歌曲的流派、相似度等，二者条目匹配
3. 30Music侧重于用户和歌曲的互动信息，可以用于用户行为分析，并利用协同过滤进行歌曲推荐
4. 歌词以词袋形式提供，5%的热门用词的使用量占了总体歌词90%的比例

### 分布与变化

1. 随着年代变化，歌曲的响度总体增大，行进速度略有增加，前奏和总时长也有延长的趋势，而音高变化不大
2. 艺术家统计数据在全球的分布不均，主要集中在北美和西欧发达国家，歌曲也自然以英语系歌曲为主
3. 早期流行的音乐流派如蓝调blue、爵士jazz等，在近些年已逐渐销声匿迹，摇滚rock和流行pop后来居上

