import com.datamirror.common.util.Encryptor;

/**
 * Util CLI para encriptar/desencriptar passwords con el IBM Encryptor de IIDR.
 *
 * Uso:
 *   java -cp api.jar:. IIDRCrypt encrypt <plain>
 *   java -cp api.jar:. IIDRCrypt decrypt <encoded-hex>
 *
 * El formato resultante es el mismo que IIDR usa en sus ficheros de
 * configuración y en los XML de exportación de instancias.
 */
public class IIDRCrypt {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: IIDRCrypt {encrypt|decrypt} <value>");
            System.exit(2);
        }
        String op = args[0];
        String value = args[1];
        switch (op) {
            case "encrypt":
                System.out.println(Encryptor.encryptAndEncode(value));
                break;
            case "decrypt":
                System.out.println(Encryptor.decodeAndDecrypt(value));
                break;
            default:
                System.err.println("Unknown op: " + op + " (use 'encrypt' or 'decrypt')");
                System.exit(2);
        }
    }
}
