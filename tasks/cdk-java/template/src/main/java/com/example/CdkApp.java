package com.example;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Stack;

public class CdkApp {
    public static void main(final String[] args) {
        App app = new App();
        String namePrefix = System.getenv().getOrDefault("NAME_PREFIX", "dev");
        
        // Add your stack and resources here. Use namePrefix for resource names.
        Stack stack = new Stack(app, "ExampleStack");
        // e.g. Bucket bucket = Bucket.Builder.create(stack, "Main")
        //     .bucketName(namePrefix + "-bucket")
        //     .build();
        
        app.synth();
    }
}
