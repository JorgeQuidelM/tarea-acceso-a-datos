package org.example;

import java.util.List;
import java.util.Scanner;

public class ConsoleUtils {

    public static void printMarkedTable(String title, List<String> items, String mark) {
        int maxLength = title.length();
        for (String item : items) {
            maxLength = Math.max(maxLength, item.length());
        }
        int width = maxLength + 6;
        String border = "+" + "-".repeat(width) + "+";
        System.out.println(border);
        System.out.printf("| %-" + (width - 2) + "s |%n", title);
        System.out.println(border);
        for (String item : items) {
            System.out.printf("| %s %-" + (width - mark.length() - 3) + "s |%n", mark, item);
        }
        System.out.println(border);
    }

    public static void printNumberedTable(String title, List<String> items) {
        int maxLength = title.length();
        for (String item : items) {
            maxLength = Math.max(maxLength, item.length());
        }
        int width = maxLength + 6;
        String border = "+" + "-".repeat(width) + "+";
        System.out.println(border);
        System.out.printf("| %-" + (width - 2) + "s |%n", title);
        System.out.println(border);
        for (int i = 0; i < items.size(); i++) {
            System.out.printf("| %d. %-" + (width - 5) + "s |%n", i + 1, items.get(i));
        }
        System.out.println(border);
    }

    public static String getStringInput(String mensaje) {
        Scanner input = new Scanner(System.in);
        String userInput;
        do {
            System.out.print(mensaje);
            userInput = input.nextLine().trim();
            if (userInput.isEmpty()) {
                System.out.println("No se puede enviar un valor vacío. Por favor, ingrese un valor válido.");
            }
        } while (userInput.isEmpty());
        return userInput;
    }

    public static int getNonNegativeIntInput(String mensaje) {
        Scanner scanner = new Scanner(System.in);
        int userInput;
        do {
            System.out.print(mensaje);
            while (!scanner.hasNextInt()) {
                System.out.print("Por favor, ingrese un valor entero válido: ");
                scanner.next();
            }
            userInput = scanner.nextInt();
            if (userInput < 0) {
                System.out.println("No se puede enviar un valor entero negativo. Por favor, ingrese un valor válido.");
            }
        } while (userInput < 0);
        return userInput;
    }
}
