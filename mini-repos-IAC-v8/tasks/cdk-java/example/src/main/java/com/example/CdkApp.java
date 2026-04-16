package com.example;

import software.amazon.awscdk.App;

public class CdkApp {
    public static void main(final String[] args) {
        App app = new App();
        String namePrefix = System.getenv().getOrDefault("NAME_PREFIX", "dev");
        
        new AppStack(app, "AppStack", namePrefix);
        
        app.synth();
    }
}
