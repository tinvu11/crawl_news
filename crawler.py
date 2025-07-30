import os
import asyncio
from pymongo import MongoClient
import re
from datetime import datetime
from dateutil import parser
import pytz
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy 
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    DomainFilter,
    URLPatternFilter,
    ContentTypeFilter
)
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
# markdownify được crawl4ai sử dụng nội bộ, không cần import trực tiếp
# from markdownify import markdownify as md

# Sửa lại hàm process_extracted_data để xử lý lỗi NameError và logic
def process_extracted_data(markdown_content) -> dict | None:

    # Define a timezone mapping for EDT
    tzinfos = {
        "EDT": pytz.timezone("America/New_York"),  # EDT is typically America/New_York
        # Add other timezone abbreviations if needed (e.g., "EST": pytz.timezone("America/New_York"))
    }

    # 1. Khởi tạo tất cả các biến với giá trị mặc định là None
    title = None
    author = None
    date = None
    main_image_url = None
    content = None
    iso_date = None

    # 2. Trích xuất Tiêu đề. Đây là trường bắt buộc. Nếu không có tiêu đề, bỏ qua.
    title_match = re.search(r"^# (.*)", markdown_content, re.MULTILINE)
    if not title_match:
        # Nếu không tìm thấy tiêu đề, coi như đây không phải trang bài viết hợp lệ
        return None
    title = title_match.group(1).strip()

    # 3. Trích xuất các trường khác
    image_match = re.search(r"\[\!\[.*?\]\(.*?\)\]\((.*?)\)", markdown_content)
    if image_match:
        main_image_url = image_match.group(1).strip()

    author_date_match = re.search(
        r'^# .*\n\s*(\[.+?\]\([^)]+\))\s*•\s*(.*?EDT)',
        markdown_content,
        re.MULTILINE | re.DOTALL
    )
    if author_date_match:
        author = author_date_match.group(1).strip() # [Ramish Zafar](...) 
        name_match = re.match(r'\[(.*?)\]\(.*?\)', author)
        if name_match:
            author = name_match.group(1).strip()
        date = author_date_match.group(2).strip()   # Jun 25, 2025 at 05:03am EDT
        dt = parser.parse(date, tzinfos=tzinfos)
        iso_date = dt.isoformat()
        print(f"Đã trích xuất: thời gian: {iso_date}")

    # Tìm nội dung giữa author/date và các section cuối của trang
    if author_date_match:
        # Tìm vị trí kết thúc của author/date line
        author_date_end = author_date_match.end()
        remaining_content = markdown_content[author_date_end:]
        
        # Tìm nội dung chính trước các section cuối
        content_match = re.search(
            r"(.*?)(?:### Further Reading|### Trending Stories|## Deal of the Day|## Products mentioned|### Subscribe to get|### Follow us on)",
            remaining_content,
            re.DOTALL
        )
        
        if content_match:
            print(f"Đã trích xuất nội dung bài viết từ markdown.")
            content = content_match.group(1).strip()
            # Loại bỏ các phần không cần thiết như navigation, social links, etc.
            content = re.sub(r'\[.*?\]\(javascript:.*?\)', '', content)  # Remove javascript links
            content = re.sub(r'\[.*?\]\(#.*?\)', '', content)  # Remove anchor links
            content = re.sub(r'^\s*\[.*?\]\(.*?\)\s*$', '', content, flags=re.MULTILINE)  # Remove standalone links
            # Loại bỏ các thẻ Twitter embed (figure với class wp-block-embed-twitter)
            content = re.sub(r'<figure[^>]*wp-block-embed[^>]*wp-block-embed-twitter[^>]*>.*?</figure>', '', content, flags=re.DOTALL)
            
            content = content.strip()
            
            if content and len(content) > 100:  # Chỉ chấp nhận nếu có nội dung đủ dài
                if content.__contains__("$"):
                    content = content.replace("$", "\$")
            else:
                content = None
                print(f"Cảnh báo: Nội dung quá ngắn hoặc rỗng.")
        else:
            print(f"Cảnh báo: Không tìm thấy nội dung bài viết trong markdown. URL có thể không hợp lệ.")
            print(f"Remaining content: {remaining_content[:500]}...")  # In ra 500 ký tự đầu tiên để kiểm tra
    else:
        print(f"Cảnh báo: Không tìm thấy author/date, không thể trích xuất nội dung.")

    # Chỉ trả về dictionary nếu có tiêu đề
    return {
        "title": title,
        "author": author,
        "time": iso_date,
        "image": main_image_url,
        "content": content
    }


# Hàm lưu vào mongodb (giữ nguyên)
def save_to_mongodb(data, db_collection):
    """
    Lưu một dictionary dữ liệu vào collection MongoDB.
    Sử dụng URL làm khóa để cập nhật hoặc chèn mới (upsert) nhằm tránh trùng lặp.
    """
    try:
        filter_query = {"url": data["url"]}
        update_data = {"$set": data}
        db_collection.update_one(filter_query, update_data, upsert=True)
        print(f"Đã lưu/cập nhật thành công vào MongoDB: {data['url']}")
    except Exception as e:
        print(f"Lỗi khi lưu vào MongoDB: {e}")


async def run_full_pipeline_crawler():

    # --- PHẦN CẤU HÌNH MONGODB ---
    MONGO_CONNECTION_STRING = os.environ.get('MONGO_URI')
    if not MONGO_CONNECTION_STRING:
        print("Lỗi: Biến môi trường MONGO_URI chưa được thiết lập!")
        return
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client["news-database"]
        articles_collection = db["articles"]
        print("Kết nối MongoDB thành công.")
    except Exception as e:
        print(f"Không thể kết nối tới MongoDB. Vui lòng kiểm tra lại. Lỗi: {e}")
        return
    
    # --- PHẦN CẤU HÌNH CRAWLER ĐA TRANG ---
    filter_chain = FilterChain([
        DomainFilter(allowed_domains=["wccftech.com"]),
        URLPatternFilter(
            patterns=[
                "*/category/*", "*/tag/*", "*/author/*","*/topic/*","*my.wccftech.com*", "https://wccftech.com/",
                "*/review*", "*/about*", "*/tip-us*", "*/videos*",
                "*/legal-disclaimer*", "*/moderation*", "*/privacy-policy*",
                "*/ethics-statement*", "*/jobs*", "*/advertise*", "*/contact*",
                "*/how-to*", "*cdn-cgi*", "*/page/*",
                "*.png", "*.jpg", "*.jpeg", "*.gif"
            ],
            reverse=True
        ),
        ContentTypeFilter(allowed_types=["text/html"])
    ])

    keyword_scorer = KeywordRelevanceScorer(
        keywords=["AMD", "NVIDIA", "Intel", "AI", "Apple", "Gaming", "CPU", "GPU"],
        weight=0.7
    )


    start_urls = {
        'hardware': "https://wccftech.com/topic/hardware/", 
        'games': "https://wccftech.com/topic/games/",
        'mobile': "https://wccftech.com/topic/gadgets/",
        'finance': "https://wccftech.com/topic/finance/",
        'deals': "https://wccftech.com/topic/deals/",
        'review': "https://wccftech.com/review/"
    }


    # print(f"Bắt đầu crawl từ: {start_urls}")

    async with AsyncWebCrawler() as crawler:
        try:
            # for url in start_urls:
            for category, url in start_urls.items():
                print(f"\n\033[1;32m--- Bắt đầu crawl từ: {url} ---\033[0m")
                config = CrawlerRunConfig(
                        deep_crawl_strategy=BestFirstCrawlingStrategy(
                            max_depth=1,
                            max_pages= 30, # Giới hạn 30 trang để test nhanh
                            filter_chain=filter_chain,
                            url_scorer=keyword_scorer,
                        ),
                        stream=True,
                        verbose=True
                )
                async for result in await crawler.arun(url, config=config):
                    if not result.markdown:
                        continue # Bỏ qua nếu không có nội dung markdown

                    processed_data = process_extracted_data(result.markdown)
                 
                    if processed_data['author'] is not None and processed_data['content'] is not None and processed_data['image'] is not None and processed_data['time'] is not None:
                        data_to_save = {
                            "category": category,
                            "src": "wccftech",
                            "url": result.url,
                            **processed_data
                        }
                        save_to_mongodb(data_to_save, articles_collection)

                    else:
                        print(f"Bỏ qua bài viết không hợp lệ: {result.url} - Thiếu thông tin bắt buộc.")
                        print(f"Nội dung: {processed_data}")



                    

        except Exception as e:
            print(f"Đã xảy ra lỗi nghiêm trọng trong quá trình crawl: {e}")

    print(f"\n--- HOÀN THÀNH ---")
    
if __name__ == "__main__":
    asyncio.run(run_full_pipeline_crawler())