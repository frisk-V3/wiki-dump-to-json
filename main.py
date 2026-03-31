import customtkinter as ctk
import threading
import mwxml
import json
import bz2
import os
import requests

class WikiDumpTool(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Wikipedia 解析 & 自動クリーンアップ")
        self.geometry("600x400")

        # --- 設定 ---
        # デスクトップのパスを自動取得
        desktop = os.path.expanduser("~/Desktop")
        self.temp_file = os.path.join(desktop, "temp_jawiki.xml.bz2")
        self.output_file = os.path.join(desktop, "wiki_dump.jsonl")
        self.target_url = "https://wikimedia.org"
        # ------------

        self.ui_setup()

    def ui_setup(self):
        self.label = ctk.CTkLabel(self, text="デスクトップに一時保存して解析 (完了後、元データは削除します)", font=("Yu Gothic", 13))
        self.label.pack(pady=20)
        self.start_button = ctk.CTkButton(self, text="実行開始", command=self.start_all, fg_color="blue")
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
            # 1. ダウンロード (デスクトップへ)
            self.update_status("ダウンロード中... (デスクトップに一時保存)")
            with requests.get(self.target_url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                with open(self.temp_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        # 進捗更新
                        self.after(0, lambda d=downloaded, t=total_size: self.progress.set(d/t))
            
            # 2. 解析 (デスクトップのファイルを使用)
            self.update_status("解析中... しばらくお待ちください")
            count = 0
            with bz2.open(self.temp_file, "rt", encoding="utf-8") as f_in, \
                 open(self.output_file, "w", encoding="utf-8") as f_out:
                
                dump = mwxml.Dump.from_file(f_in)
                for page in dump:
                    if page.namespace == 0:
                        revision = next(page)
                        if revision.text:
                            data = {"title": page.title, "text": revision.text[:200]}
                            f_out.write(json.dumps(data, ensure_ascii=False) + "\n")
                            count += 1
                            if count % 1000 == 0:
                                self.update_status(f"解析中: {count} 記事完了")

            self.update_status(f"成功！ {count}記事抽出しました")

            # 3. 後片付け (一時ファイルを削除)
            if os.path.exists(self.temp_file):
                self.update_status("一時ファイルを削除中...")
                os.remove(self.temp_file)
                self.update_status(f"完了！ 結果は {os.path.basename(self.output_file)} です")

        except Exception as e:
            self.update_status(f"エラー: {str(e)}")
        finally:
            self.after(0, lambda: self.start_button.configure(state="normal"))

    def update_status(self, text):
        self.after(0, lambda: self.status_label.configure(text=text))

if __name__ == "__main__":
    app = WikiDumpTool()
    app.mainloop()
