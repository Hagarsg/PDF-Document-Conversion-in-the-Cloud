//import api.AWS;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import software.amazon.awssdk.services.sqs.model.Message;

import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.util.List;
import javax.imageio.ImageIO;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

public class Worker {

    final static AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        start();
    }


    public static void start() {
        while (true) {
            // Get a message from an SQS queue
            String workersQueueUrl = aws.getQueueUrl("workersQueue");
            String responseQueueUrl = aws.getQueueUrl("responseQueue");
            try {
                List<Message> messages = aws.receiveMessages(workersQueueUrl);
                if (messages == null) continue; // No messages, keep polling
                
                // Parse the message
                for (Message m : messages){
                    String[] parts = m.body().split("\t");
                    String operation = parts[0];
                    String pdfUrl = parts[1];
                    String fileId = parts[2];
                
                    // Download the PDF file and perform operation
                    File pdfFile = downloadPDF(pdfUrl);
                    File resultFile = performOperation(operation, pdfFile);

                    // Upload the result to S3   
                    String s3ResultsPath = "results" + File.separator + m.messageId();
                    String resultS3Url = aws.uploadFileToS3(s3ResultsPath, resultFile);
                    String responseMessage = String.format("%s\t%s\t%s\t%s", operation, pdfUrl, s3ResultsPath, fileId);

                    // Send success message to the response queue and Remove the processed message from the task queue
                    aws.sendMessage(responseQueueUrl, responseMessage);
                    aws.deleteMessage(workersQueueUrl, m);

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

            } catch (Exception e) {
                e.printStackTrace();
                aws.sendMessage(responseQueueUrl, "Error processing task: " + e.getMessage());
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

    // public static void main(){
    //     File output = convertToImage("http://www.bethelnewton.org/images/Passover_Guide_BOOKLET.pdf");
        
    // }
    

                /* Repeatedly:
                ▪ Get a message from an SQS queue.
                ▪ Download the PDF file indicated in the message.
                ▪ Perform the operation requested on the file.
                ▪ Upload the resulting output file to S3.
                ▪ Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new
                image file, and the operation that was performed.
                ▪ remove the processed message from the SQS queue. */

}
