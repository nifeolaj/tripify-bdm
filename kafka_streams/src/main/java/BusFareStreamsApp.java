import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.Duration;
import java.util.*;

import java.time.LocalTime;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BusFareStreamsApp {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "busfare-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream("busfare");

        KStream<String, String> processedStream = sourceStream.flatMapValues(value -> {
            List<String> results = new ArrayList<>();
            try {
                JsonNode root = mapper.readTree(value);
                String departureCity = root.path("departure_city").asText().toLowerCase();
                String arrivalCity = root.path("arrival_city").asText().toLowerCase();
                String departureDateStr = root.path("departure_date").asText();  // ddMMyyyy
                List<JsonNode> trips = mapper.convertValue(root.path("data"), new TypeReference<List<JsonNode>>() {});

                Map<String, String> cityToCountry = Map.of(
                    "barcelona", "spain",
                    "madrid", "spain",
                    "rome", "italy",
                    "paris", "france"
                );

                String departureCountry = cityToCountry.getOrDefault(departureCity, "unknown");
                String arrivalCountry = cityToCountry.getOrDefault(arrivalCity, "unknown");

                DateTimeFormatter inputDateFmt = DateTimeFormatter.ofPattern("ddMMyyyy");
                DateTimeFormatter outputDateTimeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

                for (JsonNode trip : trips) {
                    String depStation = trip.path("Departure").asText().toLowerCase();
                    String arrStation = trip.path("Arrival").asText().toLowerCase();
                    String depTime = trip.path("Departure Time").asText();
                    String duration = trip.path("Duration").asText(); // e.g., "1:35 h"
                    String price = trip.path("Price").asText();

                    LocalDate departureDate = LocalDate.parse(departureDateStr, inputDateFmt);
                    LocalDateTime depDateTime = LocalDateTime.parse(departureDate + "T" + depTime);

                
                    String[] durParts = duration.replace(" h", "").split(":");
                    int durHours = Integer.parseInt(durParts[0]);
                    int durMins = Integer.parseInt(durParts[1]);
                    Duration dur = Duration.ofHours(durHours).plusMinutes(durMins);
                    LocalDateTime arrDateTime = depDateTime.plus(dur);
                    String formattedDuration = String.format("%d:%02d", durHours, durMins);

                    ObjectNode result = mapper.createObjectNode();
                    result.put("Type", "busfare");
                    result.put("Company", "Alsabus");
                    result.put("Departure", extractCity(depStation));
                    result.put("Departure_Country", departureCountry);
                    result.put("Departure_Station", depStation);
                    result.put("Arrival", extractCity(arrStation));
                    result.put("Arrival_Country", arrivalCountry);
                    result.put("Arrival_Station", arrStation);
                    result.put("Duration", formattedDuration);
                    result.put("Price", price);
                    result.put("Currency", "euro");
                    result.put("Departure_Time", depDateTime.format(outputDateTimeFmt) + "+02:00");
                    result.put("Arrival_Time", arrDateTime.format(outputDateTimeFmt) + "+02:00");

                    results.add(mapper.writeValueAsString(result));
                }

            } catch (Exception e) {
                e.printStackTrace(); // Optional: log for debugging
            }
            return results;
        });

        processedStream.to("busfare_preprocessed");



        KStream<String, String> trainFareStream = builder.stream("trainfare");
        KStream<String, String> processedTrainFareStream = trainFareStream.flatMapValues(value -> {
            List<String> results = new ArrayList<>();
            try {
                JsonNode root = mapper.readTree(value);
                String departureCity = root.path("departure_city").asText().toLowerCase();
                String arrivalCity = root.path("arrival_city").asText().toLowerCase();
                String departureDateStr = root.path("departure_date").asText();  // ddMMyyyy
                List<JsonNode> trips = mapper.convertValue(root.path("data"), new TypeReference<List<JsonNode>>() {});

                Map<String, String> cityToCountry = Map.of(
                    "barcelona", "spain",
                    "madrid", "spain",
                    "rome", "italy",
                    "paris", "france"
                );

                String departureCountry = cityToCountry.getOrDefault(departureCity, "unknown");
                String arrivalCountry = cityToCountry.getOrDefault(arrivalCity, "unknown");

                DateTimeFormatter inputDateFmt = DateTimeFormatter.ofPattern("ddMMyyyy");
                DateTimeFormatter outputDateTimeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

                for (JsonNode trip : trips) {
                    String depStation = trip.path("Departure").asText().toLowerCase();
                    String arrStation = trip.path("Arrival").asText().toLowerCase();
                    String depTime = trip.path("Departure Time").asText();
                    String duration = trip.path("Duration").asText(); // e.g., "1:35 h"
                    String price = trip.path("Price").asText();

                    LocalDate departureDate = LocalDate.parse(departureDateStr, inputDateFmt);
                    LocalDateTime depDateTime = LocalDateTime.parse(departureDate + "T" + depTime);

                    String[] durParts = duration.replace(" h", "").split(":");
                    int durHours = Integer.parseInt(durParts[0]);
                    int durMins = Integer.parseInt(durParts[1]);
                    Duration dur = Duration.ofHours(durHours).plusMinutes(durMins);
                    LocalDateTime arrDateTime = depDateTime.plus(dur);
                    String formattedDuration = String.format("%d:%02d", durHours, durMins);

                    ObjectNode result = mapper.createObjectNode();
                    result.put("Type", "trainfare");
                    result.put("Company", "Renfe");
                    result.put("Departure", extractCity(depStation));
                    result.put("Departure_Country", departureCountry);
                    result.put("Departure_Station", depStation);
                    result.put("Arrival", extractCity(arrStation));
                    result.put("Arrival_Country", arrivalCountry);
                    result.put("Arrival_Station", arrStation);
                    result.put("Duration", formattedDuration);
                    result.put("Price", price);
                    result.put("Currency", "euro");
                    result.put("Departure_Time", depDateTime.format(outputDateTimeFmt) + "+02:00");
                    result.put("Arrival_Time", arrDateTime.format(outputDateTimeFmt) + "+02:00");

                    results.add(mapper.writeValueAsString(result));
                }

            } catch (Exception e) {
                e.printStackTrace(); // Optional: log for debugging
            }
            return results;
        });

        processedTrainFareStream.to("trainfare_preprocessed");


        KStream<String, String> airfareStream = builder.stream("airfare");

        KStream<String, String> processedAirfareStream = airfareStream.flatMapValues(value -> {
            List<String> results = new ArrayList<>();
            try {
                JsonNode root = mapper.readTree(value);
                String departureCity = root.path("departure_city").asText().toLowerCase();
                String arrivalCity = root.path("arrival_city").asText().toLowerCase();
                String departureDateStr = root.path("departure_date").asText();  // d MMM yyyy
                List<JsonNode> flights = mapper.convertValue(root.path("data"), new TypeReference<List<JsonNode>>() {});

                Map<String, String> cityToCountry = Map.of(
                    "barcelona", "spain",
                    "madrid", "spain",
                    "rome", "italy",
                    "paris", "france"
                );

                String departureCountry = cityToCountry.getOrDefault(departureCity, "unknown");
                String arrivalCountry = cityToCountry.getOrDefault(arrivalCity, "unknown");

                DateTimeFormatter inputDateFmt = DateTimeFormatter.ofPattern("d MMM yyyy"); // Updated format
                DateTimeFormatter outputDateTimeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

                for (JsonNode flight : flights) {
                    String depStation = flight.path("Departure").asText().toLowerCase();
                    String arrStation = flight.path("Arrival").asText().toLowerCase();
                    String depTime = flight.path("departure_time").asText();
                    String duration = flight.path("duration").asText(); // e.g., "1 hr 25 min"
                    String price = flight.path("price").asText();

                    String priceWithSymbol = flight.path("price").asText();
                    price = priceWithSymbol.replaceAll("[^0-9.]", "");

                    // Parse the departure date
                    LocalDate departureDate = LocalDate.parse(departureDateStr, inputDateFmt);
                    
                    // Handle the 12-hour AM/PM format for departure time
                    depTime = depTime.replace("\u202f", " ").trim(); // Remove non-breaking space and trim

                    // Create a proper LocalTime first
                    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("h:mm a", Locale.ENGLISH);
                    LocalTime departureTime = LocalTime.parse(depTime, timeFormatter);

                    // Now combine the date and time properly
                    LocalDateTime depDateTime = LocalDateTime.of(departureDate, departureTime);
                    
                    // Parse duration (e.g., "1 hr 25 min")
                    String durationPattern = "([0-9]+) hr ([0-9]+) min";
                    Pattern pattern = Pattern.compile(durationPattern);
                    Matcher matcher = pattern.matcher(duration);
                    
                    int durHours = 0;
                    int durMins = 0;
                    if (matcher.find()) {
                        durHours = Integer.parseInt(matcher.group(1));
                        durMins = Integer.parseInt(matcher.group(2));
                    }
                    
                    Duration dur = Duration.ofHours(durHours).plusMinutes(durMins);
                    LocalDateTime arrDateTime = depDateTime.plus(dur);
                    String formattedDuration = String.format("%d:%02d", durHours, durMins);

                    // Create the result JSON node for the processed airfare data
                    ObjectNode result = mapper.createObjectNode();
                    result.put("Type", "airfare");
                    result.put("Company", flight.path("airline").asText());
                    result.put("Departure", departureCity);
                    result.put("Departure_Country", departureCountry);
                    result.put("Departure_Station", depStation);
                    result.put("Arrival", arrivalCity);
                    result.put("Arrival_Country", arrivalCountry);
                    result.put("Arrival_Station", arrStation);
                    result.put("Duration", formattedDuration);
                    result.put("Price", price);
                    result.put("Currency", "euro");
                    result.put("Departure_Time", depDateTime.format(outputDateTimeFmt) + "+02:00");
                    result.put("Arrival_Time", arrDateTime.format(outputDateTimeFmt) + "+02:00");

                    results.add(mapper.writeValueAsString(result));
                }

            } catch (Exception e) {
                e.printStackTrace(); // Optional: log for debugging
            }
            return results;
        });

        // Write to the "airfare_preprocessed" Kafka topic
        processedAirfareStream.to("airfare_preprocessed");


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String extractCity(String station) {
        // Extracts main city part from station name, similar to Spark version
        String city = station.split("[/\\s]")[0].replaceAll("[^a-z]", "");
        return city.toLowerCase();
    }
}
