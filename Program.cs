using Marten;
using Marten.Events;
using Marten.Events.Aggregation;
using Marten.Events.Daemon.Resiliency;
using Marten.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateApplicationBuilder();
host.Services.AddMarten(configure =>
{
    configure.Projections.Add<CategoryProjection>(ProjectionLifecycle.Inline);
    configure.Connection("User ID=postgres; Password=postgres; Host=localhost; Port=5433; Database=marten");
}).AddAsyncDaemon(DaemonMode.HotCold);

var app = host.Build();
if (true)
{
        var es = app.Services.GetRequiredService<IDocumentStore>();
        await es.Advanced.Clean.CompletelyRemoveAllAsync();
}

if (true)
{
    var streamIds = new List<Guid>();
    for (int i = 0; i < 5; i++)
    {
        var es = app.Services.GetRequiredService<IDocumentStore>();

        var sess = es.LightweightSession();

        var stream = sess.Events.StartStream<CategoryCreated>(new CategoryCreated(i.ToString()));
        await sess.SaveChangesAsync();
        streamIds.Add(stream.Id); 
    }

    {
        var es = app.Services.GetRequiredService<IDocumentStore>();

        var sess = es.LightweightSession();

        sess.Events.StartStream<BookCreated>(new BookCreated("Test", "Author"), new CategoryAssignedToBook(streamIds.First()));
        await sess.SaveChangesAsync();

        var cat = sess.Load<Category>(streamIds.First());
        Console.WriteLine(cat);
        Console.WriteLine(cat.Books.Count());
        foreach (var VARIABLE in cat.Books)
        {
            Console.WriteLine(VARIABLE); 
        }
    }
    
    {
        var es = app.Services.GetRequiredService<IDocumentStore>();

        var sess = es.LightweightSession();

        var bookStream = sess.Events.StartStream<BookCreated>(new BookCreated("TestTwo", "AuthorTwo"));
        await sess.SaveChangesAsync();
        Console.WriteLine("Now assigning category"); 
        sess.Events.Append(bookStream.Id, new CategoryAssignedToBook(streamIds.Skip(1).First()));
        await sess.SaveChangesAsync();

        var cat = sess.Load<Category>(streamIds.Skip(1).First());
        Console.WriteLine(cat);
        Console.WriteLine(cat.Books.Count());
        foreach (var VARIABLE in cat.Books)
        {
            Console.WriteLine(VARIABLE); 
        }
    }
    
    {
        var es = app.Services.GetRequiredService<IDocumentStore>();

        var sess = es.LightweightSession();

        var bookStream = sess.Events.StartStream<BookCreated>(new BookCreated("TestThreeUnchanged", "AuthorThreeUnchanged"));
        await sess.SaveChangesAsync();
        Console.WriteLine("Now assigning category"); 
        sess.Events.Append(bookStream.Id, new CategoryAssignedToBook(streamIds.Skip(2).First()));
        await sess.SaveChangesAsync();

        Console.WriteLine("Now changing books");
        sess.Events.Append(bookStream.Id, new BookChanged("NewName", "NewAuthor"));
        await sess.SaveChangesAsync();
        
        var cat = sess.Load<Category>(streamIds.Skip(2).First());
        Console.WriteLine(cat);
        Console.WriteLine(cat.Books.Count());
        foreach (var VARIABLE in cat.Books)
        {
            Console.WriteLine(VARIABLE); 
        }
    }




    //Console.WriteLine(stream.Id);
}


public record Category{
    public Guid Id { get; init; }
    public string Name { get; init; }
    public ICollection<Book> Books { get; init; }
}
public record Book {
    public Guid Id { get; init; }
    public string Name { get; init; }
    public string Author { get; init; }
}

public record CategoryCreated(string Name);

public record BookCreated(string Name, string Author);
public record BookChanged(string Name, string Author);

public record CategoryAssignedToBook(Guid CategoryId);

public record CategoryAssignedToBookGrouping(Book book);

public class CategoryProjection : MultiStreamProjection<Category, Guid>
{
    public CategoryProjection()
    {
            Identity<IEvent<CategoryCreated>>(x => x.StreamId);
            TransformsEvent<BookCreated>();
            TransformsEvent<BookChanged>();
            TransformsEvent<CategoryAssignedToBook>();
            //Identity<CategoryAssignedToBook>(x => x.CategoryId);
            CustomGrouping(new CustomCategoryGrouper());
    }

    public Category Create(CategoryCreated @event)
    {
        return new Category()
        {
            Name = @event.Name,
            Books = new List<Book>()
        };
    }

    /*public static CategoryAssignedToBook Apply(IEvent<CategoryAssignedToBook> @event, Category category, IDocumentSession documentSession)
    {
        return category with
        {
            Books = category.Books.Append(@event.StreamId)
        }
    }*/

    public Category Apply(IEvent<BookCreated> @event, Category category)
    {
        return category with
        {
            Books = category.Books.Append(new Book()
            {
                Id = @event.StreamId,
                Name = @event.Data.Name,
                Author = @event.Data.Author
            }).ToList()
        };
    }

    public Category Apply(IEvent<BookChanged> @event, Category category)
    {
        return category with
        {
            Books = category.Books.Select(x =>
            {
                if (x.Id == @event.StreamId)
                {

                    return x with
                    {
                        Name = @event.Data.Name,
                        Author = @event.Data.Author
                    };
                }


                return x;

            }).ToList()
        };
    }
}

public class CustomCategoryGrouper : IAggregateGrouper<Guid>
{
    public async Task Group(IQuerySession session, IEnumerable<IEvent> events, ITenantSliceGroup<Guid> grouping)
    {
        // This handles a category being assigned within the current event stream
        Console.WriteLine("Grouper Start");
        var eventsEnumerated = events.ToList();
        foreach (var e in eventsEnumerated)
        {
            Console.WriteLine(e);
        }

        // All event streamIds
        var streamIds = eventsEnumerated.Select(x => x.StreamId).ToList();
        
        // Events where category has been assigned
        var categoryEvents = eventsEnumerated.OfType<IEvent<CategoryAssignedToBook>>().Select(x => (category: x.Data.CategoryId, stream: x.StreamId)).ToList();
        
        // Stream Ids that aren't already handled by a category assignment and where the category is unknown or null
        var categoryHandledStreamIds = categoryEvents.Select(x => x.stream).ToList();
         
        // Filter all streams that we already handle
        streamIds = streamIds.Where(x => !x.In(categoryHandledStreamIds)).ToList();

        // For the streams where we don't know the category we need to query the database for it
        var categoriesForNonAssignedStreamIds = new List<(Guid category, Guid stream)>();
        foreach (var stream in streamIds)
        {
            // If there is a category that matches it will have a book with the correct id
            var category = session.Query<Category>().Where(x => x.Books.Any(y => y.Id == stream)).Select(x => x.Id).FirstOrDefault();
            if (category != Guid.Empty)
            {
                categoriesForNonAssignedStreamIds.Add((category: category, stream: stream ));
            }
        }

        
        var streamsToPullCompletely = categoryEvents.Select(x => x.stream).ToList();
        
        // Pull in all streams we are interested in, i.e. all that have a CategoryAssignedToBook event in the currently handled streams
        var streamsRawEvents = await session.Events.QueryAllRawEvents().Where(x => x.StreamId.In(streamsToPullCompletely)).ToListAsync();
        
        // Append all events in the currently handled changes
        streamsRawEvents = streamsRawEvents.Concat(eventsEnumerated.Where(x => x.StreamId.In(streamsToPullCompletely))).ToList();
        
        // For each of those streams we completed by fetching it from the Database add all events to the grouping
        foreach (var category in categoryEvents)
        {
            grouping.AddEvents(category.category, streamsRawEvents.Where(x => x.StreamId == category.stream));
        }

        // For all other events we do the same thing with the categories we fetched from the database
        foreach (var category in categoriesForNonAssignedStreamIds)
        {
            grouping.AddEvents(category.category, streamsRawEvents.Where(x => x.StreamId == category.stream));
        }
        
        Console.WriteLine("Grouper End\n");
    }
}