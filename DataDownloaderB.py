import json
import csv
import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class YubaoDataProcessor:
    def __init__(self, json_file_path, base_output_dir):
        self.json_file_path = json_file_path
        self.base_output_dir = base_output_dir
        self.base_video_url = "https://video.zhongguoyuyan.cn/video/20/"
        self.csv_output_path = os.path.join(self.base_output_dir, "yubao_wenzhou_audio.csv")
        
        if not os.path.exists(self.base_output_dir):
            os.makedirs(self.base_output_dir)

    def extract_audio_from_url(self, video_url, output_wav_path, retries=1):
        """
        不下载 mp4 文件，直接通过网络流提取高质量 wav 音频，支持失败重试
        """
        if os.path.exists(output_wav_path):
            return True # 文件已存在则跳过
        
        # 让 ffmpeg 直接抓取网络流，丢弃视频(-vn)，提取音频(-acodec pcm_s16le)保存
        # 添加 user-agent 和 referer 等防爬伪装头
        headers = "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36\r\nReferer: https://zhongguoyuyan.cn/\r\n"
        
        command = [
            'ffmpeg', 
            '-headers', headers,  # 强制加浏览器请求头
            '-y',               
            '-i', video_url,    
            '-vn',              
            '-acodec', 'pcm_s16le', 
            '-ar', '16000',     
            '-ac', '1',         
            output_wav_path
        ]
        
        import time
        for attempt in range(1, retries + 2):
            try:
                subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                logging.info(f"成功流式提取音频: {os.path.basename(output_wav_path)}")
                return True
            except subprocess.CalledProcessError:
                if attempt <= retries:
                    logging.warning(f"提取失败，正在重试(第{attempt}次): {os.path.basename(output_wav_path)}")
                    time.sleep(1)
                else:
                    logging.error(f"提取最终失败! 检查链接或网络: {video_url}")
        
        return False

    def process(self):
        """
        解析 JSON，保存 CSV，并流式提取所有音频
        """
        logging.info("开始解析语保工程 JSON 数据...")
        with open(self.json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 提取地区信息
        location_info = data.get('data', {}).get('mapLocation', {}).get('location', {})
        area_name = f"{location_info.get('province', '')}-{location_info.get('city', '')}-{location_info.get('country', '')}"
        
        resource_lists = data.get('data', {}).get('resourceList', [])
        
        # 严格对齐之前 wenzhou_audio.csv 的字段名
        fieldnames = ['序号', '地区', '字项', '义项', '读音', '备注', '本地文件名']

        # 准备写 CSV
        with open(self.csv_output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            total_items = 0
            for resource in resource_lists:
                res_type = resource.get('type', '未知类型')
                sounder = resource.get('sounder', '未知发音人')
                items = resource.get('items', [])
                
                # 只根据地区创建子文件夹
                save_dir = os.path.join(self.base_output_dir, area_name)
                os.makedirs(save_dir, exist_ok=True)

                for item in items:
                    video_id = item.get('video')
                    word_name = item.get('name', '未知').replace('/', '_').replace(' ', '')
                    iid = item.get('iid', '')
                    
                    if not video_id:
                        continue
                        
                    # 1. 处理音频提取
                    video_url = f"{self.base_video_url}{video_id}.mp4"
                    
                    # 命名格式，尽量接近之前逻辑
                    wav_filename = f"{iid}_{word_name}_{area_name}.wav"
                    output_wav_path = os.path.join(save_dir, wav_filename)
                    
                    self.extract_audio_from_url(video_url, output_wav_path)
                    
                    # 2. 梳理 CSV 数据，确保与之前的格式对接
                    # 将语保中的 "syllable" 等同于之前的 "读音"
                    # "en_name" (英文) 先映射为之前的 "义项"
                    # 发音人和类型放在备注中补充。
                    row_data = {
                        '序号': iid,
                        '地区': area_name,
                        '字项': item.get('name', ''),
                        '义项': item.get('en_name', ''),   # 借用英文作为意义或翻译项
                        '读音': item.get('syllable', ''),    # 提供温州话注音
                        '备注': f"{res_type}_{sounder} {item.get('remark', '')}".strip(),
                        '本地文件名': wav_filename
                    }
                    
                    writer.writerow(row_data)
                    total_items += 1
                    
        logging.info(f"全部完成！共处理并导出 {total_items} 条数据。CSV 与音频已保存在 {self.base_output_dir} 目录下。")


if __name__ == "__main__":
    processor = YubaoDataProcessor(
        json_file_path="DownloaderB.json", 
        base_output_dir="./audio2"
    )
    processor.process()
    # 目标网址可自行替换为实际的 JSON 文件路径和输出目录。
