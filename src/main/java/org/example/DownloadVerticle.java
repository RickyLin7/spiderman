package org.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.web.client.*;
import io.vertx.ext.web.codec.BodyCodec;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DownloadVerticle extends AbstractVerticle {
    int size = 0;
    private final String name = "无耻之徒";
    private final String output = "/Users/Joker/Downloads/movie";
    private final OpenOptions options = new OpenOptions();

    @Override
    public void start() {
        String dirPath = output + "/" + name;
        File f = new File(dirPath);
        if (!f.exists()) {
            boolean ok = f.mkdirs();
            if (!ok) {
                throw new RuntimeException("Make dirs" + dirPath + "failed!");
            }
        }

        String m3u8Url = "https://dalao.wahaha-kuyun.com/20201206/4055_c81d0698/1000k/hls/index.m3u8";
        String baseUrl = m3u8Url.substring(0, m3u8Url.lastIndexOf("/") + 1);
        System.out.println("Download baseUrl is: " + baseUrl);

        WebClientOptions options = new WebClientOptions();
        options.setTrustAll(true);
        options.setMaxPoolSize(100);
        WebClient client = WebClient.create(vertx, options);
        HttpRequest<Buffer> request = client.getAbs(m3u8Url);
        request.as(BodyCodec.string()).send(res -> {
            if (res.failed()) {
                res.cause().printStackTrace();
            } else {
                String result = res.result().body();
                String[] strings = result.split("\n");
                List<String> tsFiles = Stream.of(strings).filter(line -> line.endsWith(".ts")).collect(Collectors.toList());
                this.size = tsFiles.size();
                System.out.println("Download ts file count: " + this.size);

                List<Future> futures = new ArrayList<>(this.size);
                for (int i = 0; i < tsFiles.size(); i++) {
                    String url = baseUrl + tsFiles.get(i);
                    try {
                        Promise<Void> promise = Promise.promise();
                        request(client, url, dirPath + "/" + name + i + ".ts", promise);
                        futures.add(promise.future());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                CompositeFuture.all(futures).onComplete(ar -> {
                    if (ar.failed()) {
                        ar.cause().printStackTrace();
                    } else {
                        System.out.println("File download is over, start to merge ts files");

                        String finalName = output + "/" + name + ".mp4";
                        vertx.fileSystem().open(finalName, this.options, openResult -> {
                            if (openResult.failed()) {
                                System.err.println("open file" + finalName + "failed");
                            } else {
                                AsyncFile af = openResult.result();
                                Promise<Void> promise = Promise.promise();
                                compose(af, dirPath, 0, promise);
                                promise.future().onComplete(ar2 -> {
                                    if (ar2.failed()) {
                                        System.err.println("************* Merge mp4 failed!!! *************");
                                        ar2.cause().printStackTrace();
                                    } else {
                                        System.out.println("************* Merge mp4 success!!! *************");
                                        vertx.close();
                                    }
                                });
                            }
                        });

                    }
                });
            }

        });
    }

    private void compose(AsyncFile asyncFile, String outputPath, int currentIndex, Promise<Void> promise) {
        if (currentIndex == this.size) {
            promise.complete();
            return;
        }
        System.out.println("Merge file index: " + currentIndex);

        String fileName = outputPath + "/" + name + currentIndex + ".ts";
        FileSystem fs = vertx.fileSystem();
        fs.open(fileName, options, res -> {
            if (res.failed()) {
                promise.fail(res.cause());
            } else {
                res.result().pipe().endOnSuccess(false).to(asyncFile, ar -> {
                    if (ar.failed()) {
                        promise.fail(ar.cause());
                    } else {
                        compose(asyncFile, outputPath, currentIndex + 1, promise);
                    }
                });
            }
        });
    }

    private void request(WebClient client, String url, String fileName, Promise<Void> promise) throws IOException {
        request(client, url, fileName, promise, 3);
    }

    private void request(WebClient client, String url, String fileName, Promise<Void> promise, int retryNum) {
        client.getAbs(url).send(res -> {
            if (res.failed()) {
                System.out.println("Download " + fileName + " failed, start retry. RetryTimes: " + retryNum);
                retry(client, url, fileName, promise, res.cause(), retryNum);
            } else {
                Buffer buffer = res.result().body();
                vertx.fileSystem().writeFile(fileName, buffer, wr -> {
                    if (wr.failed()) {
                        retry(client, url, fileName, promise, wr.cause(), retryNum);
                    } else {
                        promise.complete();
                        System.out.println("Download " + fileName + " success");
                    }
                });
            }
        });
    }

    private void retry(WebClient client, String url, String fileName, Promise<Void> promise, Throwable e, int retryNum) {
        if (retryNum == 0) {
            promise.fail(e);
        } else {
            request(client, url, fileName, promise, retryNum - 1);
        }
    }
}
