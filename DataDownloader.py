import requests
from bs4 import BeautifulSoup
import logging
import time
import csv
import os

# 配置日志记录，方便排查错误
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataDownloader:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        }
        self.base_url = "https://wzhzyk.db.wzlib.cn/col/715"
        self.session = requests.Session()
        self.audio_dir = 'audio'
        if not os.path.exists(self.audio_dir):
            os.makedirs(self.audio_dir)

    def get_area_dir(self, area_name):
        """
        获取地区子目录路径并自动创建
        """
        safe_area = area_name.replace('/', '_').replace(' ', '').replace('(', '').replace(')', '')
        area_dir = os.path.join(self.audio_dir, safe_area)
        if not os.path.exists(area_dir):
            os.makedirs(area_dir)
        return area_dir

    def download_audio(self, url, filename, area='', retries=1):
        """
        下载音频文件到本地audio/地区目录，失败重试三次
        """
        if not url or not url.startswith('http'):
            return False
        dirpath = self.audio_dir
        if area:
            dirpath = self.get_area_dir(area)
        filepath = os.path.join(dirpath, filename)
        if os.path.exists(filepath):
            return True  # 已存在则跳过
        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(url, headers=self.headers, timeout=15)
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    f.write(r.content)
                logging.info(f"音频已下载: {filepath}")
                return True
            except Exception as e:
                logging.warning(f"音频下载失败(第{attempt}次): {url} -> {e}")
                time.sleep(2)
        logging.error(f"音频最终下载失败: {url}")
        return False

    def fetch_page(self, url, retries=3):
        """
        发起 GET 请求获取页面源码，包含简单的重试机制
        """
        for i in range(retries):
            try:
                logging.info(f"正在抓取: {url} (第 {i+1} 次尝试)")
                response = requests.get(url, headers=self.headers, timeout=10)
                # 检查请求是否成功（状态码 200）
                response.raise_for_status()
                # 显式指定编码，防止中文乱码（有时也可根据实际需要修改为 'utf-8' 或 'gbk'）
                response.encoding = response.apparent_encoding 
                return response.text
                
            except requests.RequestException as e:
                logging.warning(f"获取网页失败: {e}")
                time.sleep(2)  # 失败后稍作停顿再试
                
        logging.error(f"达到最大重试次数，放弃抓取: {url}")
        return None

    def parse_data(self, html_content):
        """
        解析单页所有音频及对应信息，返回列表
        """
        if not html_content:
            return []
        soup = BeautifulSoup(html_content, 'html.parser')
        data_list = []
        ul = soup.find('ul', class_='danzi-list')
        if not ul:
            return []
        for li in ul.find_all('li'):
            try:
                xuhao = li.find('div', class_='xuhao') or li.find('div', class_='xuhao font-s')
                diqu = li.find('div', class_='diqu')
                zixiang = li.find('div', class_='zixiang')
                yixiang = li.find('div', class_='yixiang')
                duyin = li.find('div', class_='duyin')
                beizhu = li.find('div', class_='beizhu')
                audio_div = li.find('div', class_='audio')
                audio_url = ''
                if audio_div and audio_div.find('source'):
                    audio_url = audio_div.find('source').get('src', '').strip()
                    if audio_url.startswith('//'):
                        audio_url = 'http:' + audio_url
                    elif audio_url.startswith('/'):
                        audio_url = 'http://wzhzyk.db.wzlib.cn' + audio_url
                # 下载音频
                audio_filename = ''
                if audio_url:
                    # 文件名格式：序号_字项_地区.wav
                    area_val = diqu.text.strip() if diqu else ''
                    base_name = f"{xuhao.text.strip() if xuhao else ''}_{zixiang.text.strip() if zixiang else ''}_{area_val}"
                    base_name = base_name.replace('/', '_').replace(' ', '').replace('(', '').replace(')', '')
                    ext = os.path.splitext(audio_url)[-1] or '.wav'
                    audio_filename = base_name + ext
                    self.download_audio(audio_url, audio_filename, area=area_val)
                data_list.append({
                    '序号': xuhao.text.strip() if xuhao else '',
                    '地区': diqu.text.strip() if diqu else '',
                    '字项': zixiang.text.strip() if zixiang else '',
                    '义项': yixiang.text.strip() if yixiang else '',
                    '读音': duyin.text.strip() if duyin else '',
                    '备注': beizhu.text.strip() if beizhu else '',
                    '本地文件名': audio_filename
                })
            except Exception as e:
                logging.warning(f"解析单条失败: {e}")
        return data_list


    def run(self, start_url=None, csv_file='output.csv'):
        """
        主执行流程，自动翻页，保存为CSV
        """
        url = start_url or self.base_url
        html_content = self.fetch_page(url)
        if not html_content:
            logging.error("首页抓取失败")
            return
        total_pages = 4439
        logging.info(f"共 {total_pages} 页")
        all_data = []
        for page in range(1, total_pages + 1):
            page_url = url if page == 1 else f"{url}/{page}"
            logging.info(f"抓取第 {page} 页: {page_url}")
            html = self.fetch_page(page_url)
            page_data = self.parse_data(html)
            all_data.extend(page_data)
            time.sleep(1)
        if all_data:
            with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=all_data[0].keys())
                writer.writeheader()
                writer.writerows(all_data)
            logging.info(f"已保存 {len(all_data)} 条到 {csv_file}，音频保存在 {self.audio_dir}/")
        else:
            logging.warning("未抓取到任何数据")

if __name__ == "__main__":
    # 目标网址可自定义
    target_url = "https://wzhzyk.db.wzlib.cn/col/715"
    downloader = DataDownloader()
    downloader.run(target_url, csv_file='wenzhou_audio.csv')
