import argparse
import asyncio
from data_pipeline.bulk_manager import upload_bulk_songs_to_s3_job, BulkRequestUploadS3, reconcile_s3_db_job
from data_pipeline.song_downloader import SmartDownloader, DownloadOptions

def main():
    parser = argparse.ArgumentParser(description="Neural Audio Fingerprinter Data Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: bulk-upload
    parser_bulk = subparsers.add_parser("bulk-upload", help="Upload bulk songs to S3 from database candidates")
    parser_bulk.add_argument("--count", type=int, required=True, help="Number of songs to upload")
    parser_bulk.add_argument("--bucket", type=str, required=True, help="S3 bucket name")
    parser_bulk.add_argument("--folder", type=str, required=True, help="S3 folder name")

    # Command: reconcile
    parser_reconcile = subparsers.add_parser("reconcile", help="Reconcile S3 files with database records")
    parser_reconcile.add_argument("--bucket", type=str, required=True, help="S3 bucket name")
    parser_reconcile.add_argument("--folder", type=str, required=True, help="S3 folder name")
    
    # Command: download-single
    parser_download = subparsers.add_parser("download-single", help="Download a single track by MBID")
    parser_download.add_argument("--mbid", type=str, required=True, help="MBID of the track")
    parser_download.add_argument("--url", type=str, required=True, help="URL to download from")
    parser_download.add_argument("--title", type=str, default="Unknown", help="Title of the track")
    parser_download.add_argument("--artist", type=str, default="Unknown", help="Artist of the track")

    args = parser.parse_args()

    if args.command == "bulk-upload":
        req = BulkRequestUploadS3(song_count=args.count, s3_bucket_name=args.bucket, s3_folder_name=args.folder)
        asyncio.run(upload_bulk_songs_to_s3_job(req))
    
    elif args.command == "reconcile":
        asyncio.run(reconcile_s3_db_job(args.bucket, args.folder))
        
    elif args.command == "download-single":
        downloader = SmartDownloader(download_dir="temp_processing")
        opts = DownloadOptions(urls=[args.url], mbid=args.mbid, title=args.title, artist=args.artist, audio_format="wav")
        result = downloader.process(opts)
        print(f"Downloaded to: {result['file_path']}")
        
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
