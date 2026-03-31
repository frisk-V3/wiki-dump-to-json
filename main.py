import customtkinter as ctk
import threading
import mwxml
import json
import bz2
import os

class WikiDumpTool(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Wikipedia JSON化ツール")
        self.geometry("500x300")

        # UI要素の配置
        self.label = ctk.CTkLabel(self, text="Wikipediaダンプ(bz2)を選択して開始")
        self.label.pack(pady=20)

        self.start_button = ctk.CTkButton(self, text="解析開始", command=self.start_process)
        self.start_button.pack(pady=10)

        self.progress = ctk.CTkProgressBar(self, width=400)
        self.progress.set(0)
        self.progress.pack(pady=20)

        self.status_label = ctk.CTkLabel(self, text="待機中...")
        self.status_label.pack(pady=10)

    def start_process(self):
        # ボタン無効化（二重押し防止）
        self.start_button.configure(state="disabled")
        # 別スレッドで重い処理を実行（C#の Task.Run 相当）
        threading.Thread(target=self.run_logic, daemon=True).start()

    def run_logic(self):
        input_file = "jawiki-latest-pages-articles.xml.bz2"
        output_file = "wiki_dump.jsonl"
        
        if not os.path.exists(input_file):
            self.update_status("ファイルが見つかりません！")
            return

        try:
            with bz2.open(input_file, "rb") as f_in, open(output_file, "w", encoding="utf-8") as f_out:
                dump = mwxml.Dump.from_file(f_in)
                count = 0
                
                for page in dump:
                    revision = next(page)
                    if page.namespace == 0 and revision.text:
                        data = {"title": page.title, "text": revision.text[:100]} # 100文字だけお試し
                        f_out.write(json.dumps(data, ensure_ascii=False) + "\n")
                        
                        count += 1
                        if count % 100 == 0: # 100件ごとにUI更新
                            self.update_status(f"{count}記事 処理中...")
                
                self.update_status(f"完了！ {count}記事出力しました")
        except Exception as e:
            self.update_status(f"エラー: {str(e)}")

    def update_status(self, text):
        # UIスレッドでラベルを更新
        self.after(0, lambda: self.status_label.configure(text=text))

if __name__ == "__main__":
    app = WikiDumpTool()
    app.mainloop()
