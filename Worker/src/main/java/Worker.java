//import api.AWS;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import software.amazon.awssdk.services.sqs.model.Message;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import javax.imageio.ImageIO;


public class Worker {

    final static AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        start();
    }


public static void start() {
    while (true) {
        // Get a message from an SQS queue
        String workersQueueUrl = aws.getQueueUrl(aws.getWorkerQueueName());
        String responsesQueueUrl = aws.getQueueUrl(aws.getResponsesQueueName());
        try {
            System.out.println("Polling SQS queue: " + workersQueueUrl); // Debug: Polling the queue
            List<Message> messages = aws.receiveMessages(workersQueueUrl);
            if (messages == null || messages.isEmpty()) {
                System.out.println("No messages in the queue, continuing to poll..."); // Debug: No messages
                continue; // No messages, keep polling
            }

            // Parse the message
            for (Message m : messages) {
                System.out.println("Received message: " + m.body()); // Debug: Show message content

                String[] parts = m.body().split("\t");
                if (parts.length < 3) {
                    System.out.println("Invalid message format (maybe inputfile parsing is wrong): " + m.body());
                    // Send an error response or skip this message
                    aws.deleteMessage(workersQueueUrl, m);
                    continue; // Skip to the next message
                }

                String operation = parts[0];
                String pdfUrl = parts[1];
                String fileId = parts[2];


                System.out.println("Parsed message -> Operation: " + operation + ", PDF URL: " + pdfUrl + ", File ID: " + fileId); // Debug: Parsing details

                // Download the PDF file and perform operation
                System.out.println("Downloading PDF from URL: " + pdfUrl); // Debug: Downloading PDF

                File pdfFile = null;
                File resultFile = null;
                try {
                    // Downloading PDF
                    pdfFile = downloadPDF(pdfUrl);

                    // Validate the downloaded file
                    if (pdfFile == null || !pdfFile.exists() || pdfFile.length() == 0) {
                        throw new IOException("Invalid or empty PDF file.");
                    }

                    // Performing Operation
                    System.out.println("Performing operation: " + operation); // Debug: Performing operation
                    resultFile = performOperation(operation, pdfFile);

                    // Upload the result to S3
                    String s3ResultsPath = aws.getResultsS3Name() + m.messageId();
                    System.out.println("Uploading result file to S3 path: " + s3ResultsPath); // Debug: Uploading to S3
                    String resultS3Url = aws.uploadFileToS3(s3ResultsPath, resultFile);
                    System.out.println("Result uploaded to S3, URL: " + resultS3Url); // Debug: Result uploaded

                    // Create Response Message
                    String responseMessage = String.format("%s\t%s\t%s\t%s", operation, pdfUrl, s3ResultsPath, fileId);

                    //Send Response Message
                    System.out.println("Sending response message: " + responseMessage); // Debug: Sending response
                    aws.sendMessage(responsesQueueUrl, responseMessage);
                } catch (IOException e) { // If file does not exist or operation failed
                    // Instead of result path, send an error message


                    System.out.println("Error occurred while processing task: " + e.getMessage()); // Debug: Error processing task
                    String errorMsg = "Error processing task: " + e.getMessage();

                    // Create Response Message with error
                    String responseMessage = String.format("%s\t%s\t%s\t%s", operation, pdfUrl, errorMsg, fileId);

                    // Send Message to Response Queue
                    System.out.println("Sending response message: " + responseMessage); // Debug: Sending response
                    aws.sendMessage(responsesQueueUrl, responseMessage);
                } finally {
                    System.out.println("Deleting processed message from workers queue: " + m.messageId()); // Debug: Deleting message
                    aws.deleteMessage(workersQueueUrl, m);
                    try { // Delete local input file
                        System.out.println("Attempting to delete PDF file: " + pdfFile.getAbsolutePath()); // Debug: Deleting PDF file
                        if (Files.deleteIfExists(pdfFile.toPath())) {
                            System.out.println("PDF file deleted successfully.");
                        } else {
                            System.out.println("PDF file did not exist.");
                        }
                    } catch (IOException e) {
                        System.out.println("Error deleting PDF file: " + e.getMessage());
                    }

                    try { // Delete local result file
                        if (resultFile != null) {
                            System.out.println("Attempting to delete result file: " + resultFile.getAbsolutePath());
                            if (Files.deleteIfExists(resultFile.toPath())) {
                                System.out.println("Result file deleted successfully.");
                            } else {
                                System.out.println("Result file did not exist.");
                            }
                        } else {
                            System.out.println("Result file is null, skipping deletion.");
                        }
                    } catch (IOException e) {
                        System.out.println("Error deleting result file: " + e.getMessage());
                    }

                }
            }

        } catch (Exception e) {
            System.out.println("Error deleting PDF file: " + e.getMessage());
        }
    }
}

    private static File downloadPDF(String pdfUrl) throws IOException {
        String outputFileName = "downloaded.pdf";
        File pdfFile = new File(outputFileName);

        // Open connection to the URL
        URL url = new URL(pdfUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);

        // Check the HTTP response code
        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("Failed to download PDF. HTTP Status: " + responseCode);
        }

        // Write the file to disk
        try (InputStream inputStream = connection.getInputStream();
             FileOutputStream fileOutputStream = new FileOutputStream(pdfFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
        }

        // Validate the file
        if (!pdfFile.exists() || pdfFile.length() == 0) {
            throw new IOException("Downloaded file is empty or does not exist.");
        }

        return pdfFile;
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

    // operations 

    private static File convertToImage(File pdfFile) throws IOException {
        PDDocument document = PDDocument.load(pdfFile);
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImage(0); // renders first page to image 
        File outputFile = new File("output.png"); // creates file (where image will be saved)
        ImageIO.write(image, "png", outputFile); // writes image to file in png format 
        document.close(); // closes PDDoc 
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
   /* Repeatedly:
    ▪ Get a message from an SQS queue.
    ▪ Download the PDF file indicated in the message.
    ▪ Perform the operation requested on the file.
    ▪ Upload the resulting output file to S3.
    ▪ Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new
    image file, and the operation that was performed.
    ▪ remove the processed message from the SQS queue. */
}
