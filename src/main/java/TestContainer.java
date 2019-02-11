import ru.nprts.ce.CeException;
import ru.nprts.ce.config.Config;
import ru.nprts.ce.container.Control;
import ru.nprts.ce.container.Logic;
import ru.nprts.ce.container.LogicContext;
import ru.nprts.ce.link.Link;
import ru.nprts.ce.link.Message;
import ru.nprts.ce.link.MessageType;
import ru.nprts.ce.parser.MessageComposer;
import ru.nprts.ce.parsers.IFieldParser;
import ru.nprts.ce.parsers.MessageParser;
import ru.nprts.ce.parsers.SchemeParser;
import ru.nprts.ce.scheme.Field;
import ru.nprts.ce.scheme.MessageDesc;
import ru.nprts.ce.scheme.Scheme;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestContainer {

    public static class Dues extends Logic {

        int fee = 0;
        long volume = 0;
        int seq = 0;

        Config config;
        LogicContext logicContext;
        Link output;
        Scheme scheme;
        MessageDesc mesDescDeal;
        MessageDesc mesDescFee;
        MessageComposer composerDeal;
        MessageComposer composerFee;
        MessageParser parserDeal;
        ByteBuffer bufDeal;
        ByteBuffer bufFee;

        @Override
        public int init(String s, LogicContext context) {

            config = context.getRootConfig();
            logicContext = context;
            output = context.getMapLinks().get("output").get(0);
            scheme = output.getScheme();

            return 0;
        }

        @Override
        public void dispose() {
        }

        @Override
        public void close() {
        }

        public int onControl(Control control, Map<String, String> messages) {
            return 0;
        }

        @Override
        public int open(String url) {

            mesDescDeal = scheme.getMessageDesc("deal");
            mesDescFee = scheme.getMessageDesc("fee");
            try {
                composerDeal = new MessageComposer(mesDescDeal);
                composerFee = new MessageComposer(mesDescFee);
            } catch (CeException e) {
                e.printStackTrace();
            }

            bufDeal = ByteBuffer.allocate(mesDescDeal.getSize());
            bufFee = ByteBuffer.allocate(mesDescFee.getSize());

            return 0;
        }

        @Override
        public int onMessage(Link link, Message message) {

            if (message.getType() != MessageType.DATA) {
                return 0;
            }
            if (link.getName().equals("flush")) {
                processFlush(link);
            } else if (link.getName().equals("deal")){
                processDeal(link, message);
            } else{
                getLogger().error("Unknown link: " + link.getName());
            }

            return 0;
        }

        public void processFlush(Link link) {

            Message message = new Message(mesDescFee);

            try{

                seq++;

                putValueInBuffer("fee", this, mesDescFee, composerFee, bufFee, (Supplier<Integer>) () -> fee);
                putValueInBuffer("volume", this, mesDescFee, composerFee, bufFee, (Supplier<Long>) () -> volume);
                message.setData(bufFee);
                message.setSeq(seq);

//                // Проверка cформированного сообщения
//                MessageParser parser = new SchemeParser(scheme).get(message);
//                System.out.println("Output mes: " + getMessageAsString(this, mesDescFee, parser));

                output.post(message);

            } catch (CeException e) {
                e.printStackTrace();
            }

            bufFee.clear();
        }

        public void processDeal(Link link, Message message) {

            try{

                Scheme schemeIn = link.getScheme();
                parserDeal = new SchemeParser(schemeIn).get(message);

                Long volumeIn = getValueFromParser("volume", this, mesDescDeal, parserDeal);
                if (volumeIn != null)
                    volume = volume + volumeIn;
                fee++;

                Message mesout = new Message(mesDescDeal);

                // Заполним исходящее сообщение входящим.
                MessageDesc mesDescIn = schemeIn.getMessageDesc(message.getName());

                fillComposerFields(this, composerDeal, parserDeal, mesDescIn, mesDescDeal, bufDeal);

                // Добавим значение для нового поля
                putValueInBuffer("other", this, mesDescDeal, composerDeal, bufDeal, (Supplier<String>) () -> "MARK");

                mesout.setData(bufDeal);
                output.post(mesout);

            } catch (CeException e) {
                e.printStackTrace();
            }

            bufDeal.clear();
        }
    }


    public static class Generator extends Logic {

        Config config;
        LogicContext logicContext;
        Link output;
        Scheme scheme;
        MessageDesc mesDescDeal;
        MessageComposer composerDeal;
        ByteBuffer bufDeal;
        Random random;

        String[] instruments;
        int[] users;
        int limit;
        int seq;

        @Override
        public int init(String s, LogicContext context) {

            // Разбор параметров
            String url = s.replaceAll("\\s+","");

            instruments = getParamURL(url, "instruments", true);

            String[] usersStr = getParamURL(url, "users", true);
            users = Arrays.asList(usersStr).stream().mapToInt(Integer::parseInt).toArray();

            String[] arrLimit = getParamURL(url, "limit", false);
            limit = (arrLimit == null || arrLimit.length == 0) ? -1 : Integer.parseInt(arrLimit[0]);

            config = context.getRootConfig();
            logicContext = context;
            output = context.getMapLinks().get("output").get(0);
            scheme = output.getScheme();

            random = new Random();

            return 0;
        }
        @Override
        public int open(String url) {

            mesDescDeal = scheme.getMessageDesc("deal");
            try {
                composerDeal = new MessageComposer(mesDescDeal);
            } catch (CeException e) {
                e.printStackTrace();
            }

            bufDeal = ByteBuffer.allocate(mesDescDeal.getSize());

            return 0;
        }


        @Override
        public int onMessage(Link link, Message message) {

            if (limit == 0)
                return 0;
            if (limit != -1)
                limit--;

            seq++;

            Message mes = new Message(mesDescDeal);

            // Сформируем исходящие сообщения.
            try{

                putValueInBuffer("instrument_id", this, mesDescDeal, composerDeal, bufDeal,
                        (Supplier<String>) () -> instruments[random.nextInt(instruments.length)]);

                putValueInBuffer("price", this, mesDescDeal, composerDeal, bufDeal,
                        (Supplier<Double>) () -> (double) Math.round((100.0 + 50.0 * random.nextDouble()) * 100.0)/100.0);

                putValueInBuffer("volume", this, mesDescDeal, composerDeal, bufDeal,
                        (Supplier<Long>) () -> (long) (10 + random.nextInt(10)));

                int idxBuyer = random.nextInt(users.length);
                putValueInBuffer("buyer", this, mesDescDeal, composerDeal, bufDeal,
                        (Supplier<Integer>) () -> users[idxBuyer]);

                putValueInBuffer("seller", this, mesDescDeal, composerDeal, bufDeal,
                        (Supplier<Integer>) () -> {
                            // Получим случайного seller не равного buyer
                            if (idxBuyer == 0)
                                return users[1 + random.nextInt(users.length - 1)];
                            if (idxBuyer == users.length - 1)
                                return users[random.nextInt(users.length - 1)];
                            int [] selWithoutBuyer = {random.nextInt(idxBuyer),
                                    idxBuyer + 1 + random.nextInt(users.length - idxBuyer - 1)};
                            return users[selWithoutBuyer[random.nextInt(2)]];
                        });

                mes.setData(bufDeal);
                mes.setSeq(seq);

//                // Проверка cформированного сообщения
//                MessageParser parser = new SchemeParser(scheme).get(mes);
//                System.out.println("Output mes: " + getMessageAsString(mesDescDeal, parser, this));

                output.post(mes);

            } catch (CeException e) {
                e.printStackTrace();
            }

            bufDeal.clear();

            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public void dispose() {
        }

        @Override
        public int onControl(Control control, Map<String, String> messages) {
            return 0;
        }
    }

    public static class Dump extends Logic {

        @Override
        public int init(String s, LogicContext context) {
            return 0;
        }

        @Override
        public int open(String url) {
            return 0;
        }

        @Override
        public int onMessage(Link link, Message message) {

            if (message.getType() != MessageType.DATA) {
                return 0;
            }

            Scheme schemeIn = link.getScheme();
            MessageDesc mesDescIn = schemeIn.getMessageDesc(message.getName());

            try{
                getLogger().info("DUMP: " + getMessageAsString(this, mesDescIn, new SchemeParser(schemeIn).get(message)));
            } catch (CeException e) {
                e.printStackTrace();
            }

            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public void dispose() {
        }

        @Override
        public int onControl(Control control, Map<String, String> messages) {
            return 0;
        }
    }

    static String getMessageAsString(Logic logic, MessageDesc mesDescIn, MessageParser parser) {

        StringBuilder sb = new StringBuilder();

        sb.append("{");
        for (Field field : mesDescIn.getFields()) {
            sb.append(field.getName()).append(" : ");
            sb.append(TestContainer.<Object>getValueFromParser(field.getName(), logic, mesDescIn, parser));
            sb.append(", ");
        }

        sb.deleteCharAt(sb.length() - 2);
        sb.append("}");

        return sb.toString();
    }

    static void putValueInBuffer(String name, Logic logic, MessageDesc mesDesc, MessageComposer composer,
                                        ByteBuffer buff, Supplier<?> func){

        Field field = mesDesc.getField(name);

        if (field == null){
            logic.getLogger().error("В схеме отсутствует поле с именем: " + name);
            return;
        }
        try {
            switch (field.getType()) {
                case INT8:
                    composer.putByte(name, (Byte) func.get(), buff);
                    break;
                case INT16:
                    composer.putShort(name, (Short) func.get(), buff);
                    break;
                case INT32:
                    composer.putInt(name, (Integer) func.get(), buff);
                    break;
                case INT64:
                    composer.putLong(name, (Long) func.get(), buff);
                    break;
                case BYTES:
                    composer.putBytes(name, (byte[]) func.get(), buff);
                    break;
                case DECIMAL:
                    composer.putDecimal(name, (BigDecimal) func.get(), buff);
                    break;
                case DOUBLE:
                    composer.putDouble(name, (Double) func.get(), buff);
                    break;
                case STRING:
                case VSTRING:
                    composer.putString(name, (String) func.get(), buff);
                    break;
                case ENUM:
                case MESSAGE:
                    throw new RuntimeException("I wasn't taught to work with these types");
                default:
                    throw new RuntimeException("Invalid field type");
            }

        } catch (CeException e) {
            e.printStackTrace();
        }

    }

    // Generic на будущее, чтобы не делать cast, и присваивать напрямую (Long vol = getValueFromParser(...);)
    // Но следует понимать, что не все типы могут быть приведены друг к другу.
    static <T> T getValueFromParser(String name, Logic logic, MessageDesc mesDesc, MessageParser parser){

        Field field = mesDesc.getField(name);

        if (field == null){
            logic.getLogger().error("В схеме отсутствует поле с именем: " + name);
            return null;
        }

        IFieldParser fieldParser = parser.get(name);

        if (fieldParser == null){
            logic.getLogger().error("Не удалось прочитать сообщение поля: " + name);
            return null;
        }

        try{
            switch (field.getType()) {
                case INT8:
                    return (T) (Byte) fieldParser.asByte();
                case INT16:
                    return (T) (Short) fieldParser.asShort();
                case INT32:
                    return (T) (Integer) fieldParser.asInt();
                case INT64:
                    return (T) (Long) fieldParser.asLong();
                case BYTES:
                    return (T) fieldParser.bytes();
                case DECIMAL:
                    return (T) fieldParser.asBigDecimal();
                case DOUBLE:
                    return (T) (Double) fieldParser.asDouble();
                case STRING:
                case VSTRING:
                    return (T) fieldParser.asString();
                case ENUM:
                case MESSAGE:
                    throw new RuntimeException("I wasn't taught to work with these types");
                default:
                    throw new RuntimeException("Invalid field type");
            }
        } catch (CeException e) {
            e.printStackTrace();
        }
        return null;
    }

    static void fillComposerFields(Logic logic, MessageComposer composer, MessageParser parser,
                                   MessageDesc mesDescIn, MessageDesc mesDescOut, ByteBuffer buff) {

        for (Field field : mesDescIn.getFields()) {

            Field fieldOut = mesDescOut.getField(field.getName());

            if (fieldOut == null || fieldOut.getType() != field.getType())
                continue;

            putValueInBuffer(fieldOut.getName(), logic, mesDescOut, composer, buff,
                    () -> getValueFromParser(fieldOut.getName(), logic, mesDescIn, parser));
        }
    }

    static String[] getParamURL(String url, String name, boolean mandatoryField){

        // Получим массив строковых значений из параметра
        String[] arrStr = null;

        // При помощи регулярных выражений выделим из строки все что идет после имени параметра
        // и до ";" (или до конца строки если параметр последний).
        StringBuilder sb = new StringBuilder();
        String pattern = sb.append("(").append(name).append("=)(.*?)((;)|$)").toString();
        Matcher matcher = Pattern.compile(pattern).matcher(url);

        // Преобразуем полученную строку в массив строк. Разделителем является ","
        if (matcher.find() && matcher.groupCount() > 1){
            arrStr = Arrays.stream(matcher.group(2).split(",")).map(String::trim).toArray(String[]::new);
        }

        // Бросим исключение если параметр обязательный
        if ((arrStr == null || arrStr.length == 0) && mandatoryField){
            sb.setLength(0);
            throw new RuntimeException(sb.append("Need ").append(name).toString());
        }

        return arrStr;
    }
}
