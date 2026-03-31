import customtkinter as ctk
import threading
import mwxml
import json
import bz2
import io
import requests
from azure.storage.blob import BlobServiceClient # pip install azure-storage-blob

class WikiDumpTool(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Wikipedia Blob連携ツール")
        self.geometry("600x400")

        # --- 設定（本来は環境変数や設定ファイルから取得） ---
        self.connection_string = "YOUR_AZURE_CONNECTION_STRING"
        self.container_name = "wiki-temp"
        self.blob_name = "jawiki-latest.xml.bz2"
        self.target_url = "https://wikimedia.org"
        # ----------------------------------------------

        self.ui_setup()

    def ui_setup(self):
        self.label = ctk.CTkLabel(self, text="ネット → Blobストレージ → 解析 (ローカル保存なし)", font=("Yu Gothic", 14))
        self.label.pack(pady=20)
        self.start_button = ctk.CTkButton(self, text="Blob経由で解析開始", command=self.start_all)
        self.start_button.pack(pady=10)
        self.progress = ctk.CTkProgressBar(self, width=450)
        self.progress.set(0)
        self.progress.pack(pady=20)
        self.status_label = ctk.CTkLabel(self, text="待機中...")
        self.status_label.pack(pady=10)

    def start_all(self):
        self.start_button.configure(state="disabled")
        threading.Thread(target=self.main_workflow, daemon=True).start()

    def main_workflow(self):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            blob_client = blob_service_client.get_blob_client(container=self.container_name, blob=self.blob_name)

            # 1. ネットからBlobへ直接アップロード (Stream転送)
            self.update_status("ネットからBlobへ転送中...")
            response = requests.get(self.target_url, stream=True)
            # dataはイテレータとして渡すことでメモリを節約
            blob_client.upload_blob(response.iter_content(chunk_size=1024*1024), overwrite=True)

            # 2. Blobから読み込みながら解析 (Stream解析)
            self.update_status("Blobから読み取り解析中...")
            download_stream = blob_client.download_blob().chunks()
            
            # mwxmlに渡すためにバイナリストリーム化
            # ※完全なメモリ節約のためには、BytesIOではなくカスタムラッパーが必要な場合があります
            raw_data = io.BytesIO(blob_client.download_blob().readall()) 
            
            count = 0
            with bz2.open(raw_data, "rt", encoding="utf-8") as f_in, \
                 open("wiki_dump.jsonl", "w", encoding="utf-8") as f_out:
                
                dump = mwxml.Dump.from_file(f_in)
                for page in dump:
                    if page.namespace == 0:
                        revision = next(page)
                        if revision.text:
                            data = {"title": page.title, "text": revision.text[:100]}
                            f_out.write(json.dumps(data, ensure_ascii=False) + "\n")
                            count += 1
                            if count % 100 == 0:
                                self.update_status(f"解析中: {count} 記事")

            self.update_status(f"完了！Blob経由で {count} 記事処理しました")
            
            # 3. 使い終わったらBlobを削除（一時利用の場合）
            # blob_client.delete_blob()

        except Exception as e:
            self.update_status(f"エラー: {str(e)}")
        finally:
            self.after(0, lambda: self.start_button.configure(state="normal"))

    def update_status(self, text):
        self.after(0, lambda: self.status_label.configure(text=text))

if __name__ == "__main__":
    app = WikiDumpTool()
    app.mainloop()
