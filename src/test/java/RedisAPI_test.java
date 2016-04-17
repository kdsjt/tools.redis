import com.iecas.kds.tools.redis.RedisAPI;

/**
 * Created by Administrator on 2016/4/8.
 */
public class RedisAPI_test {

    public static void main(String[] args) {
        RedisAPI.set("test","123");
        RedisAPI.incr("test");
        System.out.println(RedisAPI.get("test"));
    }
}
