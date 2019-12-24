import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 乐 on 2019/12/24.
 */
public class HbaseTest {
    public static void main(String[] args) throws IOException {
        HbaseControl hbaseControl=new HbaseControl();
        List<String> family=new ArrayList<>();
        family.add("Location");
        family.add("Score");
        hbaseControl.createTable("ZJL",family);

    }
}
