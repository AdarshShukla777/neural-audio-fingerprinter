import os
import time
import logging
import numpy as np
import librosa
import tritonclient.grpc as grpcclient

logger = logging.getLogger("Processor")
logging.basicConfig(level=logging.INFO)

class FingerprintGenerator:
    def __init__(self):
        # We now connect to the Triton server instead of loading TensorFlow locally
        self.triton_url = os.getenv("TRITON_URL", "localhost:8001")
        self.model_name = "fingerprinter"
        
        logger.info(f"⏳ Connecting to Triton Inference Server at: {self.triton_url}")
        
        try:
            self.triton_client = grpcclient.InferenceServerClient(url=self.triton_url)
            if not self.triton_client.is_server_live():
                logger.warning("⚠️ Triton server is not live yet!")
            else:
                logger.info("✅ Connected to Triton Inference Server")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Triton Server: {e}")
            self.triton_client = None

        # --- AUDIO CONFIG ---
        self.SAMPLE_RATE = 8000
        self.N_MELS = 256
        self.N_FFT = 1024
        self.HOP_LEN = 256
        self.MODEL_TIME_FRAMES = 34

        self.INFERENCE_BATCH_SIZE = 64

    def process_audio(self, audio_data: bytes):
        """Generates fingerprints using Triton Inference Server."""
        start_total = time.perf_counter()
        
        import tempfile
        try:
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
                f.write(audio_data)
                f.flush()
                temp_file = f.name
                
            y, sr = librosa.load(temp_file, sr=self.SAMPLE_RATE, mono=True)
            if len(y) == 0:
                return []
                
            mels = librosa.feature.melspectrogram(
                y=y, sr=sr, n_mels=self.N_MELS, n_fft=self.N_FFT, hop_length=self.HOP_LEN
            )
            mels = librosa.power_to_db(mels, ref=np.max)
            mels = (mels - np.mean(mels)) / (np.std(mels) + 1e-9)

        except Exception as e:
            logger.error(f"Audio processing error: {e}")
            return []
        finally:
            if 'temp_file' in locals() and os.path.exists(temp_file):
                os.unlink(temp_file)

        total_frames = mels.shape[1]
        segments = []
        offsets = []
        stride = self.MODEL_TIME_FRAMES // 2

        if total_frames < self.MODEL_TIME_FRAMES:
            pad_width = self.MODEL_TIME_FRAMES - total_frames
            mels = np.pad(mels, ((0, 0), (0, pad_width)), mode='constant')
            total_frames = mels.shape[1]

        for i in range(0, total_frames - self.MODEL_TIME_FRAMES, stride):
            window = mels[:, i:i + self.MODEL_TIME_FRAMES]
            segments.append(window.reshape(256, 34, 1))
            offsets.append((i * self.HOP_LEN) / self.SAMPLE_RATE)

        if not segments:
            return []

        if not self.triton_client:
            logger.error("Triton client is not initialized!")
            return []

        fingerprints = []
        inference_start = time.perf_counter()

        try:
            for i in range(0, len(segments), self.INFERENCE_BATCH_SIZE):
                batch = np.array(segments[i:i + self.INFERENCE_BATCH_SIZE], dtype=np.float32)
                
                # Triton Inputs
                inputs = [grpcclient.InferInput("input_1", batch.shape, "FP32")]
                inputs[0].set_data_from_numpy(batch)
                
                # Triton Outputs
                outputs = [grpcclient.InferRequestedOutput("output_0")]
                
                # Inference request
                results = self.triton_client.infer(
                    model_name=self.model_name,
                    inputs=inputs,
                    outputs=outputs
                )
                
                embeddings = results.as_numpy("output_0")
                
                # Normalize embeddings
                norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
                embeddings /= (norms + 1e-9)

                for j, emb in enumerate(embeddings):
                    fingerprints.append({
                        "vector": emb.tolist(),
                        "offset": offsets[i + j]
                    })
        except Exception as e:
            logger.error(f"Triton inference failed: {e}")
            return []

        inference_time = time.perf_counter() - inference_start
        total_time = time.perf_counter() - start_total

        logger.info(
            f"⚡ Triton Inference done | Segments={len(segments)} | "
            f"Inference={inference_time:.3f}s | Total={total_time:.3f}s"
        )

        return fingerprints
