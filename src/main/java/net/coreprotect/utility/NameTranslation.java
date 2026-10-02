package net.coreprotect.utility;

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bukkit.Material;
import org.bukkit.entity.EntityType;

/**
 * Block/item/entity names that players see are wrapped in an inline marker holding the Minecraft translation key
 * and the plain fallback text. The marker is turned into a client-side translatable component when the message is
 * sent to a player, and replaced by the plain text for the console and for every other plain-text path.
 */
public final class NameTranslation {

    public static final char OPEN = (char) 0xE000;
    public static final char SEPARATOR = (char) 0xE001;
    public static final char CLOSE = (char) 0xE002;

    private static final Pattern MARKER = Pattern.compile(OPEN + "([^" + SEPARATOR + CLOSE + "]*)" + SEPARATOR + "([^" + CLOSE + "]*)" + CLOSE);

    private NameTranslation() {
        throw new IllegalStateException("Utility class");
    }

    public static String marker(String key, String fallback) {
        return OPEN + key + SEPARATOR + fallback + CLOSE;
    }

    public static boolean containsMarker(String text) {
        return text != null && text.indexOf(OPEN) > -1;
    }

    /**
     * Replaces every marker with its plain fallback text.
     */
    public static String strip(String text) {
        if (!containsMarker(text)) {
            return text;
        }

        return MARKER.matcher(text).replaceAll(match -> Matcher.quoteReplacement(match.group(2)));
    }

    /**
     * Replaces every marker with the given placeholder character and returns the translation keys in order.
     */
    public static String replaceMarkers(String text, char placeholder, List<String> keys) {
        Matcher matcher = MARKER.matcher(text);
        StringBuilder result = new StringBuilder();
        int last = 0;
        while (matcher.find()) {
            result.append(text, last, matcher.start()).append(placeholder);
            keys.add(matcher.group(1));
            last = matcher.end();
        }

        return result.append(text, last, text.length()).toString();
    }

    /**
     * Block or item name (e.g. "stone", "minecraft:oak_log") for chat. Unknown, legacy and non-vanilla names are returned unchanged.
     */
    public static String block(String name) {
        try {
            String id = vanillaId(name);
            if (id != null) {
                Material material = Material.getMaterial(id.toUpperCase(Locale.ROOT));
                if (material != null && !material.isLegacy()) {
                    return marker(material.translationKey(), name);
                }
            }
        }
        catch (RuntimeException | LinkageError e) {
            // fall back to the plain name
        }

        return name;
    }

    /**
     * Entity type name (e.g. "zombie") for chat. Unknown names (including player names) are returned unchanged.
     */
    public static String entity(String name) {
        try {
            String id = vanillaId(name);
            if (id != null) {
                EntityType type = EntityType.valueOf(id.toUpperCase(Locale.ROOT));
                if (type != EntityType.UNKNOWN) {
                    return marker(type.translationKey(), name);
                }
            }
        }
        catch (RuntimeException | LinkageError e) {
            // fall back to the plain name
        }

        return name;
    }

    private static String vanillaId(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }

        String id = name.startsWith("minecraft:") ? name.substring("minecraft:".length()) : name;
        if (id.isEmpty() || id.indexOf(':') > -1 || id.indexOf(' ') > -1 || id.charAt(0) == '#') {
            return null;
        }

        return id;
    }
}
