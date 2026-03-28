package com.example.pulumi;

import com.pulumi.Pulumi;

/**
 * Pulumi Java app entrypoint. Define your stack here.
 * Use NAME_PREFIX from environment (or Pulumi config namePrefix) for resource names.
 */
public class App {
    public static void main(String[] args) {
        Pulumi.run(ctx -> {
            // Add your stack and resources here.
            // Example: S3 bucket with name prefix from env/config for collision avoidance.
        });
    }
}
