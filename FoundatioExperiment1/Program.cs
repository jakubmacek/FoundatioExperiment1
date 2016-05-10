using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Storage;
using StackExchange.Redis;

//https://github.com/exceptionless/Foundatio
namespace FoundatioExperiment1
{
    class Program
    {
        static ConnectionMultiplexer ConnectRedis()
        {
            return ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                EndPoints = { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 6379) },
                AllowAdmin = true
            });
        }

        static void ClearRedis()
        {
            using (var redisConnection = ConnectRedis())
            {
                redisConnection.GetServer(redisConnection.GetEndPoints()[0]).FlushAllDatabases();
            }
        }

        static void CacheExperiment()
        {
            using (var redisConnection = ConnectRedis())
            using (var baseCache = new RedisHybridCacheClient(redisConnection))
            using (var cache = new ScopedCacheClient(baseCache, "cachePrefix"))
            {
                var terminate = false;

                var createDataTask = Task.Factory.StartNew(async delegate
                {
                    int counter = 0;
                    while (!terminate)
                    {
                        var data = new VeryExpensiveData { Counter = ++counter, Date = DateTime.Now };
                        Console.WriteLine("cache.SetAsync VeryExpensiveData {0}", data.Counter);
                        await cache.SetAsync("VeryExpensiveData", data);
                        await Task.Delay(3000);
                    }
                    return counter;
                });

                var useDataTask = Task.Factory.StartNew(async delegate
                {
                    while (!terminate)
                    {
                        var data = await cache.GetAsync<VeryExpensiveData>("VeryExpensiveData");
                        if (data.HasValue)
                            Console.WriteLine("cache.GetAsync VeryExpensiveData {0} {1}", data.Value.Counter, data.Value.Date);
                        await Task.Delay(1200);
                    }
                });

                Thread.Sleep(10000);
                Console.WriteLine("Set terminate = true and wait.");
                terminate = true;
                Task.WaitAll(createDataTask, useDataTask);
            }
        }

        static void ShowImage(string title, Image image)
        {
            var form = new Form();
            form.Text = title;
            form.ClientSize = new Size(400, 400);
            form.FormBorderStyle = FormBorderStyle.FixedDialog;
            //form.StartPosition = FormStartPosition.CenterScreen;
            form.MinimizeBox = false;
            form.MaximizeBox = false;
            var pictureBox = new PictureBox();
            pictureBox.SizeMode = PictureBoxSizeMode.CenterImage;
            pictureBox.Image = image;
            pictureBox.Dock = DockStyle.Fill;
            form.Controls.Add(pictureBox);
            form.ShowDialog();
        }

        static IFileStorage CreateFileStorage()
        {
            return new FolderFileStorage(@"D:\FoundatioExperiment1\Storage");
        }

        static void JobExperiment()
        {
            var showThumbnailTaskCancellationTokenSource = new CancellationTokenSource();
            var showThumbnailTaskCancellationToken = showThumbnailTaskCancellationTokenSource.Token;
            var showThumbnailTask = Task.Factory.StartNew(delegate
            {
                using (var redisConnection = ConnectRedis())
                using (var messageBus = new RedisMessageBus(redisConnection.GetSubscriber()))
                {
                    messageBus.Subscribe<ShowThumbnailMessage>(async message =>
                    {
                        using (var fileStorage = CreateFileStorage())
                        {
                            var bytes = await fileStorage.GetFileContentsRawAsync(message.ThumbnailImagePath);
                            using (var stream = new MemoryStream(bytes))
                            using (var image = Image.FromStream(stream))
                            {
                                ShowImage(message.ThumbnailImagePath, image);
                            }
                        }
                    });

                    showThumbnailTaskCancellationToken.WaitHandle.WaitOne();
                }
            });

            var handleCreateThumbnailJobsTaskCancellationTokenSource = new CancellationTokenSource();
            var handleCreateThumbnailJobsTask = Task.Factory.StartNew(async delegate
            {
                using (var redisConnection = ConnectRedis())
                using (var fileStorage = CreateFileStorage())
                using (var queue = new RedisQueue<CreateThumbnailJobData>(redisConnection, queueName: "CreateThumbnail"))
                using (var messageBus = new RedisMessageBus(redisConnection.GetSubscriber()))
                {
                    var createThumbnailJob = new CreateThumbnailJob(queue, fileStorage, messageBus);
                    await createThumbnailJob.RunContinuousAsync(cancellationToken: handleCreateThumbnailJobsTaskCancellationTokenSource.Token);
                }
            });

            var loadOriginalImagesAndCreateJobsTask = Task.Factory.StartNew(async delegate
            {
                using (var redisConnection = ConnectRedis())
                using (var fileStorage = CreateFileStorage())
                using (var createThumbnailQueue = new RedisQueue<CreateThumbnailJobData>(redisConnection, queueName: "CreateThumbnail"))
                {
                    var photosDirectory = new DirectoryInfo(@"X:\work\@\photos");
                    foreach (var photoFile in photosDirectory.GetFiles())
                    {
                        if (photoFile.Extension == ".jpg" || photoFile.Extension == ".png" || photoFile.Extension == ".gif")
                        {
                            using (var photoFileStream = new FileStream(photoFile.FullName, FileMode.Open, FileAccess.Read))
                            {
                                var job = new CreateThumbnailJobData();
                                job.Name = photoFile.Name;
                                job.OriginalImagePath = Guid.NewGuid().ToString();
                                job.ThumbnailImagePath = Guid.NewGuid() + ".png";
                                if (await fileStorage.SaveFileAsync(job.OriginalImagePath, photoFileStream))
                                    await createThumbnailQueue.EnqueueAsync(job);
                                Console.WriteLine("Job queued: {0}", job.Name);
                            }
                        }
                    }
                }
            });

            Task.WaitAll(loadOriginalImagesAndCreateJobsTask);
            Console.WriteLine("All jobs queued.");
            Thread.Sleep(10000);
            handleCreateThumbnailJobsTaskCancellationTokenSource.Cancel();
            Task.WaitAll(handleCreateThumbnailJobsTask);
        }

        static void Main(/*string[] args*/)
        {
            Console.WriteLine("Started.");
            ClearRedis();
            //CacheExperiment();
            JobExperiment();

            //TODO Locks
            //TODO Metrics
            //TODO Logging

            Console.WriteLine("Stopped.");
            Console.WriteLine("Press 'enter' to close.");
            Console.ReadLine();
        }
    }

    class VeryExpensiveData
    {
        public int Counter { get; set; }
        public DateTime Date { get; set; }
    }

    public class CreateThumbnailJob : QueueJobBase<CreateThumbnailJobData>, IDisposable
    {
        IFileStorage _fileStorage;
        IMessageBus _messageBus;

        public CreateThumbnailJob(IQueue<CreateThumbnailJobData> queue, IFileStorage fileStorage, IMessageBus messageBus)
            : base(queue)
        {
            if (fileStorage == null)
                throw new ArgumentNullException(nameof(fileStorage));
            if (messageBus == null)
                throw new ArgumentNullException(nameof(messageBus));
            _fileStorage = fileStorage;
            _messageBus = messageBus;
        }

        public void Dispose()
        {
            if (_fileStorage != null)
            {
                _fileStorage.Dispose();
                _fileStorage = null;
            }
        }

        protected override async Task<JobResult> ProcessQueueEntryAsync(QueueEntryContext<CreateThumbnailJobData> context)
        {
            var originalImageStream = await _fileStorage.GetFileStreamAsync(context.QueueEntry.Value.OriginalImagePath);
            var originalImage = Image.FromStream(originalImageStream);
            originalImageStream.Dispose();
            Console.WriteLine("Original loaded: {0}", context.QueueEntry.Value.Name);
            await Task.Delay(500);

            int thumbnailWidth = 200, thumbnailHeight = 200;
            if (originalImage.Width > originalImage.Height)
                thumbnailHeight = originalImage.Height * thumbnailWidth / originalImage.Width;
            else
                thumbnailWidth = originalImage.Width * thumbnailHeight / originalImage.Height;

            var thumbnailImage = ResizeImage(originalImage, thumbnailWidth, thumbnailHeight);
            Console.WriteLine("Thumbnail created: {0}", context.QueueEntry.Value.Name);
            await Task.Delay(500);

            using (var thumbnailImageStream = new MemoryStream())
            {
                thumbnailImage.Save(thumbnailImageStream, ImageFormat.Png);
                await _fileStorage.SaveFileAsync(context.QueueEntry.Value.ThumbnailImagePath, thumbnailImageStream);
            }
            Console.WriteLine("Thumbnail saved: {0}", context.QueueEntry.Value.Name);
            await Task.Delay(500);

            await _messageBus.PublishAsync(new ShowThumbnailMessage() { ThumbnailImagePath = context.QueueEntry.Value.ThumbnailImagePath });

            return JobResult.Success;
        }

        /// <summary>
        /// Resize the image to the specified width and height.
        /// </summary>
        /// <param name="image">The image to resize.</param>
        /// <param name="width">The width to resize to.</param>
        /// <param name="height">The height to resize to.</param>
        /// <returns>The resized image.</returns>
        private static Bitmap ResizeImage(Image image, int width, int height)
        {
            var destRect = new Rectangle(0, 0, width, height);
            var destImage = new Bitmap(width, height);

            destImage.SetResolution(image.HorizontalResolution, image.VerticalResolution);

            using (var graphics = Graphics.FromImage(destImage))
            {
                graphics.CompositingMode = CompositingMode.SourceCopy;
                graphics.CompositingQuality = CompositingQuality.HighQuality;
                graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
                graphics.SmoothingMode = SmoothingMode.HighQuality;
                graphics.PixelOffsetMode = PixelOffsetMode.HighQuality;

                using (var wrapMode = new ImageAttributes())
                {
                    wrapMode.SetWrapMode(WrapMode.TileFlipXY);
                    graphics.DrawImage(image, destRect, 0, 0, image.Width, image.Height, GraphicsUnit.Pixel, wrapMode);
                }
            }

            return destImage;
        }
    }

    public class CreateThumbnailJobData
    {
        public string Name { get; set; }

        public string OriginalImagePath { get; set; }

        public string ThumbnailImagePath { get; set; }
    }

    public class ShowThumbnailMessage
    {
        public string ThumbnailImagePath { get; set; }
    }
}
