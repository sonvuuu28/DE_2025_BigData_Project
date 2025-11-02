import streamlit as st
import subprocess
import psutil
import os

st.title("🎵 Music Vote Streaming Control Panel")

# 👉 Chỉnh path Spark tại đây (nếu khác thì sửa lại)
SPARK_HOME = r"C:\spark\spark-3.5.0-bin-hadoop3"
SPARK_SUBMIT = os.path.join(SPARK_HOME, "bin", "spark-submit.cmd")


def run_spark_stream():
    subprocess.Popen(
        f'"{SPARK_SUBMIT}" --master local[2] stream.py',
        shell=True,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )


if st.button("Start API"):
    subprocess.Popen(["python", "api.py"])
    st.success("✅ API Started")

if st.button("Save Vote Raw"):
    subprocess.Popen(["python", "get_data.py"])
    st.success("📌 Poller Started – generating votes...")

if st.button("Start Spark Stream"):
    cmd = 'start cmd /k "spark-submit --master local[2] spark_stream.py"'
    subprocess.Popen(cmd, shell=True)
    st.success("⚡ Spark Streaming Started (check new CMD window)")


if st.button("Stop All"):
    for proc in psutil.process_iter():
        try:
            cmd = " ".join(proc.cmdline())
            if (
                "api.py" in cmd
                or "get_data.py" in cmd
                or "spark_stream.py" in cmd
                or "spark-submit" in cmd
            ):
                proc.kill()
        except:
            pass
    st.warning("🛑 All jobs stopped")
