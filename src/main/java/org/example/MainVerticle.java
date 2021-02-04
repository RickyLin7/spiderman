package org.example;

import io.vertx.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {
    public void start() {
        vertx.deployVerticle(DownloadVerticle.class.getName());
    }
}
