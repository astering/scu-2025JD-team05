import requests
import json


def xiecheng(dcity, acity, date):
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
        "Content-Type": "application/json",
        "referer": r"https://flights.ctrip.com/itinerary/oneway/SHA-TAO?date=2022-10-10"
    }

    city = {'阿尔山': 'YIE', '阿克苏': 'AKU', '阿拉善右旗': 'RHT', '阿拉善左旗': 'AXF', '阿勒泰': 'AAT', '阿里': 'NGQ',
            '澳门': 'MFM',
            '安庆': 'AQG', '安顺': 'AVA', '鞍山': 'AOG', '巴彦淖尔': 'RLK', '百色': 'AEB', '包头': 'BAV', '保山': 'BSD',
            '北海': 'BHY',
            '北京': 'BJS', '白城': 'DBC', '白山': 'NBS', '毕节': 'BFJ', '博乐': 'BPL', '重庆': 'CKG', '昌都': 'BPX',
            '常德': 'CGD',
            '常州': 'CZX', '朝阳': 'CHG', '成都': 'CTU', '池州': 'JUH', '赤峰': 'CIF', '揭阳': 'SWA', '长春': 'CGQ',
            '长沙': 'CSX',
            '长治': 'CIH', '承德': 'CDE', '沧源': 'CWJ', '达县': 'DAX', '大理': 'DLU', '大连': 'DLC', '大庆': 'DQA',
            '大同': 'DAT',
            '丹东': 'DDG', '稻城': 'DCY', '东营': 'DOY', '敦煌': 'DNH', '芒市': 'LUM', '额济纳旗': 'EJN',
            '鄂尔多斯': 'DSN', '恩施': 'ENH',
            '二连浩特': 'ERL', '佛山': 'FUO', '福州': 'FOC', '抚远': 'FYJ', '阜阳': 'FUG', '赣州': 'KOW',
            '格尔木': 'GOQ', '固原': 'GYU',
            '广元': 'GYS', '广州': 'CAN', '贵阳': 'KWE', '桂林': 'KWL', '哈尔滨': 'HRB', '哈密': 'HMI', '海口': 'HAK',
            '海拉尔': 'HLD',
            '邯郸': 'HDG', '汉中': 'HZG', '杭州': 'HGH', '合肥': 'HFE', '和田': 'HTN', '黑河': 'HEK', '呼和浩特': 'HET',
            '淮安': 'HIA',
            '怀化': 'HJJ', '黄山': 'TXN', '惠州': 'HUZ', '鸡西': 'JXA', '济南': 'TNA', '济宁': 'JNG', '加格达奇': 'JGD',
            '佳木斯': 'JMU',
            '嘉峪关': 'JGN', '金昌': 'JIC', '金门': 'KNH', '锦州': 'JNZ', '嘉义': 'CYI', '西双版纳': 'JHG',
            '建三江': 'JSJ', '晋江': 'JJN',
            '井冈山': 'JGS', '景德镇': 'JDZ', '九江': 'JIU', '九寨沟': 'JZH', '喀什': 'KHG', '凯里': 'KJH',
            '康定': 'KGT', '克拉玛依': 'KRY',
            '库车': 'KCA', '库尔勒': 'KRL', '昆明': 'KMG', '拉萨': 'LXA', '兰州': 'LHW', '黎平': 'HZH', '丽江': 'LJG',
            '荔波': 'LLB',
            '连云港': 'LYG', '六盘水': 'LPF', '临汾': 'LFQ', '林芝': 'LZY', '临沧': 'LNJ', '临沂': 'LYI', '柳州': 'LZH',
            '泸州': 'LZO',
            '洛阳': 'LYA', '吕梁': 'LLV', '澜沧': 'JMJ', '龙岩': 'LCX', '满洲里': 'NZH', '梅州': 'MXZ', '绵阳': 'MIG',
            '漠河': 'OHE',
            '牡丹江': 'MDG', '马祖': 'MFK', '南昌': 'KHN', '南充': 'NAO', '南京': 'NKG', '南宁': 'NNG', '南通': 'NTG',
            '南阳': 'NNY',
            '宁波': 'NGB', '宁蒗': 'NLH', '攀枝花': 'PZI', '普洱': 'SYM', '齐齐哈尔': 'NDG', '黔江': 'JIQ',
            '且末': 'IQM', '秦皇岛': 'BPE',
            '青岛': 'TAO', '庆阳': 'IQN', '衢州': 'JUZ', '日喀则': 'RKZ', '日照': 'RIZ', '三亚': 'SYX', '厦门': 'XMN',
            '上海': 'SHA',
            '深圳': 'SZX', '神农架': 'HPG', '沈阳': 'SHE', '石家庄': 'SJW', '塔城': 'TCG', '台州': 'HYN', '太原': 'TYN',
            '扬州': 'YTY',
            '唐山': 'TVS', '腾冲': 'TCZ', '天津': 'TSN', '天水': 'THQ', '通辽': 'TGO', '铜仁': 'TEN', '吐鲁番': 'TLQ',
            '万州': 'WXN',
            '威海': 'WEH', '潍坊': 'WEF', '温州': 'WNZ', '文山': 'WNH', '乌海': 'WUA', '乌兰浩特': 'HLH',
            '乌鲁木齐': 'URC', '无锡': 'WUX',
            '梧州': 'WUZ', '武汉': 'WUH', '武夷山': 'WUS', '西安': 'SIA', '西昌': 'XIC', '西宁': 'XNN',
            '锡林浩特': 'XIL',
            '香格里拉(迪庆)': 'DIG',
            '襄阳': 'XFN', '兴义': 'ACX', '徐州': 'XUZ', '香港': 'HKG', '烟台': 'YNT', '延安': 'ENY', '延吉': 'YNJ',
            '盐城': 'YNZ',
            '伊春': 'LDS',
            '伊宁': 'YIN', '宜宾': 'YBP', '宜昌': 'YIH', '宜春': 'YIC', '义乌': 'YIW', '银川': 'INC', '永州': 'LLF',
            '榆林': 'UYN',
            '玉树': 'YUS',
            '运城': 'YCU', '湛江': 'ZHA', '张家界': 'DYG', '张家口': 'ZQZ', '张掖': 'YZY', '昭通': 'ZAT', '郑州': 'CGO',
            '中卫': 'ZHY',
            '舟山': 'HSN',
            '珠海': 'ZUH', '遵义(茅台)': 'WMT', '遵义(新舟)': 'ZYI'}

    url = 'https://flights.ctrip.com/online/list/oneway-bjs-sha?depdate=2026-01-30&cabin=y_s_c_f&adult=1&child=0&infant=0&source_module=recommend_card'
    request_payload = {"flightWay": "Oneway",
                       "classType": "ALL",
                       "hasChild": 'false',
                       "hasBaby": 'false',
                       "searchIndex": 1,
                       "airportParams": [
                           {"dcity": city.get(dcity), "acity": city.get(acity), "dcityname": dcity, "acityname": acity,
                            "date": date}]}

    response = requests.post(url, data=json.dumps(request_payload), headers=headers).text
    try:
        routeList = json.loads(response).get('data').get('routeList')
    except:
        pass

    try:
        for route in routeList:
            if len(route.get('legs')) == 1:
                info = {}
                legs = route.get('legs')[0]
                flight = legs.get('flight')
                airlineName = flight.get('airlineName')
                flightNumber = flight.get('flightNumber')
                airportName = flight.get('departureAirportInfo').get('airportName')
                departureDate = flight.get('departureDate')
                arrivalDate = flight.get('arrivalDate')
                punctualityRate = flight.get('punctualityRate')
                cabins = legs.get('cabins')[0].get('price')
                price = cabins.get('price')
                # 打印出来
                print(dcity, acity, airlineName, flightNumber, airportName, departureDate, arrivalDate, punctualityRate,
                      price)
    except:
        pass


if __name__ == "__main__":
    # 热门城市
    # all_citys=['北京','上海','广州','深圳' ,'成都' ,'杭州', '武汉','西安', '重庆','青岛','长沙','南京','厦门','昆明','大连','天津','郑州','三亚','济南','福州']
    all_citys = ['北京', '上海', '广州', '深圳', '成都', '杭州', '武汉', '西安', '重庆', '青岛', '长沙', '南京', '厦门',
                 '昆明', '大连', '天津', '郑州',
                 '三亚', '济南', '福州',
                 '阿勒泰', '阿克苏', '鞍山', '安庆', '安顺', '阿拉善左', '中国澳门', '阿里', '阿拉善右', '阿尔山',
                 '巴中', '百色', '包头', '毕节', '北海', '博乐', '保山', '白城', '布尔津', '白山', '巴彦淖尔',
                 '昌都', '承德', '常德', '长春', '朝阳', '赤峰', '长治', '沧源', '常州', '池州',
                 '大同', '达州', '稻城', '丹东', '迪庆', '大理', '敦煌', '东营', '大庆', '德令哈',
                 '鄂尔多斯', '额济纳旗', '恩施', '二连浩特',
                 '阜阳', '抚远', '富蕴', '佛山',
                 '果洛', '格尔木', '广元', '固原', '中国高雄', '赣州', '贵阳', '桂林',
                 '红原', '海口', '河池', '邯郸', '黑河', '呼和浩特', '合肥', '淮安', '怀化', '海拉尔', '哈密', '衡阳',
                 '哈尔滨', '和田', '花土沟', '中国花莲',
                 '霍林郭勒', '惠州', '汉中', '黄山', '呼伦贝尔', '菏泽',
                 '中国嘉义', '景德镇', '加格达奇', '嘉峪关', '井冈山', '金昌', '九江', '荆门', '佳木斯', '济宁', '锦州',
                 '建三江', '鸡西', '九寨沟', '中国金门',
                 '揭阳', '荆州',
                 '库车', '康定', '喀什', '凯里', '库尔勒', '克拉玛依',
                 '黎平', '澜沧', '龙岩', '临汾', '兰州', '丽江', '荔波', '吕梁', '临沧', '陇南', '六盘水', '拉萨',
                 '洛阳', '连云港', '临沂', '柳州', '泸州',
                 '林芝',
                 '芒市', '牡丹江', '中国马祖', '绵阳', '梅州', '中国马公', '满洲里', '漠河',
                 '南昌', '中国南竿', '南充', '宁波', '宁蒗', '南宁', '南阳', '南通',
                 '攀枝花', '普洱', '琼海', '秦皇岛', '祁连', '且末', '庆阳', '黔江', '泉州', '衢州', '齐齐哈尔', '日照',
                 '日喀则', '若羌', '神农架',
                 '莎车', '沈阳', '石河子', '石家庄', '上饶', '三明', '十堰', '邵阳', '松原', '台州', '中国台中', '塔城',
                 '腾冲', '铜仁',
                 '通辽', '天水', '吐鲁番', '通化', '中国台南', '中国台北', '中国台东', '唐山', '太原',
                 '五大连池', '乌兰浩特', '乌兰察布', '乌鲁木齐', '潍坊', '威海', '文山', '温州', '乌海', '武夷山',
                 '无锡', '梧州', '万州', '乌拉特中',
                 '巫山', '武隆', '芜湖', '兴义', '夏河', '中国香港', '西双版纳', '新源', '忻州', '信阳', '襄阳', '西昌',
                 '锡林浩特', '西宁', '徐州',
                 '延安', '银川', '伊春', '永州', '榆林', '宜宾', '运城', '宜春', '宜昌', '伊宁', '义乌', '营口', '延吉',
                 '烟台', '盐城', '扬州', '玉树玉',
                 '林岳阳',
                 '张家界', '舟山', '扎兰屯', '张掖', '昭通', '湛江', '中卫', '张家口', '珠海', '遵义']

    date = "20221120"
    citysCount = len(all_citys)
    for city1 in all_citys:
        for city2 in all_citys:
            if (city1 != city2):
                xiecheng(city1, city2, date)