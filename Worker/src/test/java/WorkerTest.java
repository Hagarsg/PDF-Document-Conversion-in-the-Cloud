import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

public class WorkerTest { // Class name should follow camel case convention (e.g., WorkerTest)
    public static void main(String[] args) {
        // Simulate SQS message
        String messageBody = "ToImage\thttp://www.bethelnewton.org/images/Passover_Guide_BOOKLET.pdf";
        try {
            start(messageBody);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // operations
    public static void start(String messageBody) throws IOException, InterruptedException {
        while (true) {
            // Parse the message
            String[] parts = messageBody.split("\t");
            String operation = parts[0];
            String pdfUrl = parts[1];

            // Log the URL and operation being processed
            System.out.println("Processing operation: " + operation);
            System.out.println("PDF URL: " + pdfUrl);

            // Download the PDF file and perform operation
            File pdfFile = downloadPDF(pdfUrl);
            if (pdfFile == null) {
                System.out.println("Failed to download the PDF from URL: " + pdfUrl);
                continue; // Skip this iteration if the download failed
            }
            System.out.println("Downloaded PDF file: " + pdfFile.getAbsolutePath());

            File resultFile = performOperation(operation, pdfFile);
            System.out.println("Operation result saved to: " + resultFile.getAbsolutePath());

            // Upload the result to S3
            String responseMessage = String.format("%s\t%s", operation, pdfUrl);
            System.out.println(responseMessage);

            try { // delete local output file in order to handle next message
                if (Files.deleteIfExists(pdfFile.toPath())) {
                    System.out.println("File deleted successfully.");
                } else {
                    System.out.println("File did not exist.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred: " + e.getMessage());
            }

            try { // delete local output file in order to handle next message
                if (Files.deleteIfExists(resultFile.toPath())) {
                    System.out.println("File deleted successfully.");
                } else {
                    System.out.println("File did not exist.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred: " + e.getMessage());
            }
        }
    }

    private static File downloadPDF(String pdfUrl) throws IOException, InterruptedException {
        Path pdfPath = Path.of("downloaded.pdf");
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(pdfUrl))
                .GET()
                .build();
        httpClient.send(request, HttpResponse.BodyHandlers.ofFile(pdfPath));
        return pdfPath.toFile();
    }

    private static File performOperation(String operation, File pdfFile) throws IOException {
        switch (operation) {
            case "ToImage":
                return convertToImage(pdfFile);
            case "ToHTML":
                return convertToHTML(pdfFile);
            case "ToText":
                return convertToText(pdfFile);
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

//    private static File convertToImage(File pdfFile) throws IOException {
//        PDDocument document = PDDocument.load(pdfFile);
//        PDFRenderer renderer = new PDFRenderer(document);
//        BufferedImage image = renderer.renderImage(0); // renders first page to image
//        File outputFile = new File("output.png"); // creates file (where image will be saved)
//        ImageIO.write(image, "png", outputFile); // writes image to file in png format
//        document.close(); // closes PDDoc
//        return outputFile;
//    }
    private static File convertToImage(File pdfFile) throws IOException {
        // Log the start of the conversion process
        System.out.println("Starting conversion for PDF: " + pdfFile.getAbsolutePath());

        // Load the PDF document
        System.out.println("Loading PDF document...");
        PDDocument document = PDDocument.load(pdfFile);
        if (document.isEncrypted()) {
            System.out.println("Warning: The PDF is encrypted.");
        } else {
            System.out.println("PDF document loaded successfully.");
        }

        // Initialize PDFRenderer and render the first page
        System.out.println("Rendering the first page of the PDF to an image...");
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImage(0); // renders first page to image
        System.out.println("First page rendered to image successfully.");

        // Define the output file for the image
        File outputFile = new File("output.png");
        System.out.println("Saving the rendered image to file: " + outputFile.getAbsolutePath());

        // Write the image to the file in PNG format
        try {
            ImageIO.write(image, "png", outputFile);
            System.out.println("Image saved successfully to: " + outputFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error writing image to file: " + outputFile.getAbsolutePath());
            throw e; // Re-throw to propagate the error
        }

        // Close the PDF document
        document.close();
        System.out.println("PDF document closed.");

        // Return the output file
        return outputFile;
    }

    private static File convertToHTML(File pdfFile) throws IOException {
        PDDocument document = PDDocument.load(pdfFile);
        PDFTextStripper stripper = new PDFTextStripper(); // extracts textual content from pdf file
        String text = stripper.getText(document);
        File outputFile = new File("output.html");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write("<html><body><pre>" + text + "</pre></body></html>");
        }
        document.close();
        return outputFile;
    }

    private static File convertToText(File pdfFile) throws IOException {
        PDDocument document = PDDocument.load(pdfFile);
        PDFTextStripper stripper = new PDFTextStripper();
        String text = stripper.getText(document);
        File outputFile = new File("output.txt");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(text);
        }
        document.close();
        return outputFile;
    }
}
